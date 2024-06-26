#include "storage/undo_buffer.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

struct UndoRecordHeader {
    UndoBuffer::UndoRecordType recordType;
    uint32_t recordSize;

    UndoRecordHeader(const UndoBuffer::UndoRecordType recordType, const uint32_t recordSize)
        : recordType{recordType}, recordSize{recordSize} {}
};

struct CatalogEntryRecord {
    CatalogSet* catalogSet;
    CatalogEntry* catalogEntry;
};

struct SequenceEntryRecord {
    SequenceCatalogEntry* sequenceEntry;
    SequenceChangeData sequenceChangeData;
    int64_t prevVal;
};

template<typename F>
void UndoBufferIterator::iterate(F&& callback) {
    idx_t bufferIdx = 0;
    while (bufferIdx < undoBuffer.memoryBuffers.size()) {
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        const auto end = current + currentBuffer.getCurrentPosition();
        while (current < end) {
            UndoRecordHeader recordHeader = *reinterpret_cast<UndoRecordHeader const*>(current);
            current += sizeof(UndoRecordHeader);
            callback(recordHeader.recordType, current);
            current += recordHeader.recordSize; // Skip the current entry.
        }
        bufferIdx++;
    }
}

template<typename F>
void UndoBufferIterator::reverseIterate(F&& callback) {
    idx_t numBuffersLeft = undoBuffer.memoryBuffers.size();
    while (numBuffersLeft > 0) {
        const auto bufferIdx = numBuffersLeft - 1;
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        const auto end = current + currentBuffer.getCurrentPosition();
        std::vector<std::pair<UndoBuffer::UndoRecordType, uint8_t const*>> entries;
        while (current < end) {
            UndoRecordHeader recordHeader = *reinterpret_cast<UndoRecordHeader const*>(current);
            current += sizeof(UndoRecordHeader);
            entries.push_back({recordHeader.recordType, current});
            current += recordHeader.recordSize; // Skip the current entry.
        }
        for (auto i = entries.size(); i >= 1; i--) {
            callback(entries[i - 1].first, entries[i - 1].second);
        }
        numBuffersLeft--;
    }
}

UndoBuffer::UndoBuffer(ClientContext& clientContext) : clientContext{clientContext} {}

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(CatalogEntryRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::CATALOG_ENTRY, sizeof(CatalogEntryRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const CatalogEntryRecord catalogEntryRecord{&catalogSet, &catalogEntry};
    *reinterpret_cast<CatalogEntryRecord*>(buffer) = catalogEntryRecord;
}

void UndoBuffer::createSequenceChange(SequenceCatalogEntry& sequenceEntry, const SequenceData& data,
    int64_t prevVal) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(SequenceEntryRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::SEQUENCE_ENTRY,
        sizeof(SequenceEntryRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const SequenceEntryRecord sequenceEntryRecord{&sequenceEntry,
        {data.usageCount, data.currVal, data.nextVal}, prevVal};
    *reinterpret_cast<SequenceEntryRecord*>(buffer) = sequenceEntryRecord;
}

uint8_t* UndoBuffer::createUndoRecord(uint64_t size) {
    if (memoryBuffers.empty() || !memoryBuffers.back().canFit(size)) {
        auto capacity = UndoMemoryBuffer::UNDO_MEMORY_BUFFER_SIZE;
        while (size > capacity) {
            capacity *= 2;
        }
        // We need to allocate a new memory buffer.
        memoryBuffers.push_back(UndoMemoryBuffer(capacity));
    }
    auto res = memoryBuffers.back().getDataUnsafe() + memoryBuffers.back().getCurrentPosition();
    memoryBuffers.back().moveCurrentPosition(size);
    return res;
}

void UndoBuffer::commit(transaction_t commitTS) const {
    UndoBufferIterator iterator{*this};
    iterator.iterate([&](UndoRecordType entryType, uint8_t const* entry) {
        commitRecord(entryType, entry, commitTS);
    });
}

void UndoBuffer::rollback() {
    UndoBufferIterator iterator{*this};
    iterator.reverseIterate(
        [&](UndoRecordType entryType, uint8_t const* entry) { rollbackEntry(entryType, entry); });
}

void UndoBuffer::commitRecord(UndoRecordType entryType, const uint8_t* entry,
    transaction_t commitTS) const {
    switch (entryType) {
    case UndoRecordType::CATALOG_ENTRY:
        commitCatalogEntry(entry, commitTS);
        break;
    case UndoRecordType::SEQUENCE_ENTRY:
        commitSequenceEntry(entry, commitTS);
        break;
    default:
        KU_UNREACHABLE;
    }
}

void UndoBuffer::rollbackEntry(UndoRecordType entryType, const uint8_t* entry) {
    switch (entryType) {
    case UndoRecordType::CATALOG_ENTRY:
        rollbackCatalogEntry(entry);
        break;
    case UndoRecordType::SEQUENCE_ENTRY:
        rollbackSequenceEntry(entry);
        break;
    default:
        KU_UNREACHABLE;
    }
}

void UndoBuffer::commitCatalogEntry(const uint8_t* record, transaction_t commitTS) const {
    const auto& [_, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto newCatalogEntry = catalogEntry->getNext();
    KU_ASSERT(newCatalogEntry);
    newCatalogEntry->setTimestamp(commitTS);
    auto& wal = clientContext.getStorageManager()->getWAL();
    switch (newCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY: {
        if (catalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY) {
            KU_ASSERT(catalogEntry->isDeleted());
            wal.logCreateCatalogEntryRecord(newCatalogEntry);
        } else {
            // Must be alter
            KU_ASSERT(catalogEntry->getType() == newCatalogEntry->getType());
            const auto tableEntry = catalogEntry->constPtrCast<TableCatalogEntry>();
            wal.logAlterTableEntryRecord(tableEntry->getAlterInfo());
        }
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
    case CatalogEntryType::SEQUENCE_ENTRY:
    case CatalogEntryType::TYPE_ENTRY: {
        KU_ASSERT(
            catalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY && catalogEntry->isDeleted());
        wal.logCreateCatalogEntryRecord(newCatalogEntry);
    } break;
    case CatalogEntryType::DUMMY_ENTRY: {
        KU_ASSERT(newCatalogEntry->isDeleted());
        switch (catalogEntry->getType()) {
        // Eventually we probably want to merge these
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_TABLE_ENTRY:
        case CatalogEntryType::REL_GROUP_ENTRY:
        case CatalogEntryType::RDF_GRAPH_ENTRY: {
            const auto tableCatalogEntry = catalogEntry->constPtrCast<TableCatalogEntry>();
            wal.logDropCatalogEntryRecord(tableCatalogEntry->getTableID(), catalogEntry->getType());
        } break;
        case CatalogEntryType::SEQUENCE_ENTRY: {
            auto sequenceCatalogEntry = catalogEntry->constPtrCast<SequenceCatalogEntry>();
            wal.logDropCatalogEntryRecord(sequenceCatalogEntry->getSequenceID(),
                catalogEntry->getType());
        } break;
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
            // DO NOTHING. We don't persistent function entries.
        } break;
        // TODO: Add support for dropping macros and types.
        case CatalogEntryType::SCALAR_MACRO_ENTRY:
        case CatalogEntryType::TYPE_ENTRY:
        default: {
            throw RuntimeException(stringFormat("Not supported catalog entry type {} yet.",
                CatalogEntryTypeUtils::toString(catalogEntry->getType())));
        }
        }
    } break;
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
        // DO NOTHING. We don't persistent function entries.
    } break;
    default: {
        throw RuntimeException(stringFormat("Not supported catalog entry type {} yet.",
            CatalogEntryTypeUtils::toString(catalogEntry->getType())));
    }
    }
}

void UndoBuffer::rollbackCatalogEntry(const uint8_t* entry) {
    const auto& [catalogSet, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(entry);
    const auto entryToRollback = catalogEntry->getNext();
    KU_ASSERT(entryToRollback);
    if (entryToRollback->getNext()) {
        // If entryToRollback has a newer entry (next) in the version chain. Simple remove
        // entryToRollback from the chain.
        const auto newerEntry = entryToRollback->getNext();
        newerEntry->setPrev(entryToRollback->movePrev());
    } else {
        // This is the begin of the version chain.
        auto olderEntry = entryToRollback->movePrev();
        catalogSet->erase(catalogEntry->getName());
        if (olderEntry) {
            catalogSet->emplace(std::move(olderEntry));
        }
    }
}

void UndoBuffer::commitSequenceEntry(const uint8_t* entry, transaction_t /* commitTS */) const {
    const auto& sequenceRecord = *reinterpret_cast<SequenceEntryRecord const*>(entry);
    const auto sequenceEntry = sequenceRecord.sequenceEntry;
    auto& wal = clientContext.getStorageManager()->getWAL();
    wal.logUpdateSequenceRecord(sequenceEntry->getSequenceID(), sequenceRecord.sequenceChangeData);
}

void UndoBuffer::rollbackSequenceEntry(const uint8_t* entry) {
    const auto& sequenceRecord = *reinterpret_cast<SequenceEntryRecord const*>(entry);
    const auto sequenceEntry = sequenceRecord.sequenceEntry;
    sequenceEntry->replayVal(sequenceRecord.sequenceChangeData.usageCount - 1,
        sequenceRecord.prevVal, sequenceRecord.sequenceChangeData.currVal);
}

} // namespace storage
} // namespace kuzu
