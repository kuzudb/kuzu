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

template<typename F>
void UndoBufferIterator::iterate(F&& callback) {
    uint8_t const* end;
    common::idx_t bufferIdx = 0;
    while (bufferIdx < undoBuffer.memoryBuffers.size()) {
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        end = current + currentBuffer.getCurrentPosition();
        while (current < end) {
            UndoBuffer::UndoEntryType entryType =
                *reinterpret_cast<UndoBuffer::UndoEntryType const*>(current);
            current += sizeof(UndoBuffer::UndoEntryType);
            auto entrySize = *reinterpret_cast<uint32_t const*>(current);
            current += sizeof(uint32_t); // Skip entrySize field.
            callback(entryType, current);
            current += entrySize; // Skip the current entry.
        }
        bufferIdx++;
    }
}

template<typename F>
void UndoBufferIterator::reverseIterate(F&& callback) {
    uint8_t const* end;
    common::idx_t numBuffersLeft = undoBuffer.memoryBuffers.size();
    while (numBuffersLeft > 0) {
        auto bufferIdx = numBuffersLeft - 1;
        auto& currentBuffer = undoBuffer.memoryBuffers[bufferIdx];
        auto current = currentBuffer.getData();
        end = current + currentBuffer.getCurrentPosition();
        std::vector<uint8_t const*> entries;
        std::vector<UndoBuffer::UndoEntryType> entryTypes;
        while (current < end) {
            UndoBuffer::UndoEntryType entryType =
                *reinterpret_cast<UndoBuffer::UndoEntryType const*>(current);
            entryTypes.push_back(entryType);
            current += sizeof(UndoBuffer::UndoEntryType);
            auto entrySize = *reinterpret_cast<uint32_t const*>(current);
            current += sizeof(uint32_t); // Skip entrySize field.
            entries.push_back(current);
            current += entrySize; // Skip the current entry.
        }
        for (auto i = entries.size(); i >= 1; i--) {
            callback(entryTypes[i - 1], entries[i - 1]);
        }
        numBuffersLeft--;
    }
}

UndoBuffer::UndoBuffer(ClientContext& clientContext) : clientContext{clientContext} {}

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) {
    // We store an pointer to catalog entry inside the undo buffer.
    // Each catalog undo entry has the following format:
    //      [entryType: UndoEntryType][entrySize: uint32_t][pointer: CatalogEntry*][ponter:
    //      CatalogSet*]
    auto buffer = createUndoEntry(
        sizeof(UndoEntryType) + sizeof(uint32_t) + sizeof(CatalogEntry*) + sizeof(CatalogSet*));
    *reinterpret_cast<UndoEntryType*>(buffer) = UndoEntryType::CATALOG_ENTRY;
    buffer += sizeof(UndoEntryType);
    *reinterpret_cast<uint32_t*>(buffer) = sizeof(CatalogEntry*) + sizeof(CatalogSet*);
    buffer += sizeof(uint32_t);
    *reinterpret_cast<CatalogEntry**>(buffer) = &catalogEntry;
    buffer += sizeof(CatalogEntry*);
    *reinterpret_cast<CatalogSet**>(buffer) = &catalogSet;
}

void UndoBuffer::createSequenceChange(SequenceCatalogEntry& sequenceEntry, const SequenceData& data,
    int64_t prevVal) {
    // Each sequence undo entry has the following format:
    //      [entryType: UndoEntryType][entrySize: uint32_t]
    //      [pointer: SequenceCatalogEntry*][usageCount: uint64_t][currVal: int64_t][nextVal:
    //      int64_t]
    auto buffer =
        createUndoEntry(sizeof(UndoEntryType) + sizeof(uint32_t) + sizeof(SequenceCatalogEntry*) +
                        sizeof(uint64_t) + sizeof(int64_t) * 3);
    *reinterpret_cast<UndoEntryType*>(buffer) = UndoEntryType::SEQUENCE_ENTRY;
    buffer += sizeof(UndoEntryType);
    *reinterpret_cast<uint32_t*>(buffer) =
        sizeof(SequenceCatalogEntry*) + sizeof(uint64_t) + sizeof(int64_t) * 3;
    buffer += sizeof(uint32_t);
    *reinterpret_cast<SequenceCatalogEntry**>(buffer) = &sequenceEntry;
    buffer += sizeof(SequenceCatalogEntry*);
    *reinterpret_cast<uint64_t*>(buffer) = data.usageCount;
    buffer += sizeof(uint64_t);
    *reinterpret_cast<int64_t*>(buffer) = data.currVal;
    buffer += sizeof(int64_t);
    *reinterpret_cast<int64_t*>(buffer) = data.nextVal;
    buffer += sizeof(int64_t);
    *reinterpret_cast<int64_t*>(buffer) = prevVal;
}

uint8_t* UndoBuffer::createUndoEntry(uint64_t size) {
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

void UndoBuffer::commit(transaction_t commitTS) {
    UndoBufferIterator iterator{*this};
    iterator.iterate([&](UndoEntryType entryType, uint8_t const* entry) {
        commitEntry(entryType, entry, commitTS);
    });
}

void UndoBuffer::rollback() {
    UndoBufferIterator iterator{*this};
    iterator.reverseIterate(
        [&](UndoEntryType entryType, uint8_t const* entry) { rollbackEntry(entryType, entry); });
}

void UndoBuffer::commitEntry(UndoEntryType entryType, const uint8_t* entry,
    transaction_t commitTS) {
    switch (entryType) {
    case UndoEntryType::CATALOG_ENTRY:
        commitCatalogEntry(entry, commitTS);
        break;
    case UndoEntryType::SEQUENCE_ENTRY:
        commitSequenceEntry(entry, commitTS);
        break;
    default:
        KU_UNREACHABLE;
    }
}

void UndoBuffer::rollbackEntry(UndoEntryType entryType, const uint8_t* entry) {
    switch (entryType) {
    case UndoEntryType::CATALOG_ENTRY:
        rollbackCatalogEntry(entry);
        break;
    case UndoEntryType::SEQUENCE_ENTRY:
        rollbackSequenceEntry(entry);
        break;
    default:
        KU_UNREACHABLE;
    }
}

void UndoBuffer::commitCatalogEntry(const uint8_t* entry, transaction_t commitTS) {
    auto& catalogEntry = *reinterpret_cast<CatalogEntry* const*>(entry);
    auto newCatalogEntry = catalogEntry->getNext();
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
            auto tableEntry = catalogEntry->constPtrCast<TableCatalogEntry>();
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
            auto tableCatalogEntry = catalogEntry->constPtrCast<TableCatalogEntry>();
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
    auto& catalogEntry = *reinterpret_cast<CatalogEntry* const*>(entry);
    auto& catalogSet = *reinterpret_cast<CatalogSet* const*>(entry + sizeof(CatalogEntry*));
    auto entryToRollback = catalogEntry->getNext();
    KU_ASSERT(entryToRollback);
    if (entryToRollback->getNext()) {
        // If entryToRollback has a newer entry (next) in the version chain. Simple remove
        // entryToRollback from the chain.
        auto newerEntry = entryToRollback->getNext();
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

void UndoBuffer::commitSequenceEntry(const uint8_t* entry, transaction_t /* commitTS */) {
    auto& sequenceEntry = *reinterpret_cast<SequenceCatalogEntry* const*>(entry);
    entry += sizeof(SequenceCatalogEntry*);
    auto& usageCount = *reinterpret_cast<uint64_t const*>(entry);
    entry += sizeof(uint64_t);
    auto& currVal = *reinterpret_cast<int64_t const*>(entry);
    entry += sizeof(int64_t);
    auto& nextVal = *reinterpret_cast<int64_t const*>(entry);
    auto& wal = clientContext.getStorageManager()->getWAL();
    wal.logUpdateSequenceRecord(sequenceEntry->getSequenceID(), {usageCount, currVal, nextVal});
}

void UndoBuffer::rollbackSequenceEntry(const uint8_t* entry) {
    auto& sequenceEntry = *reinterpret_cast<SequenceCatalogEntry* const*>(entry);
    entry += sizeof(SequenceCatalogEntry*);
    auto& usageCount = *reinterpret_cast<uint64_t const*>(entry);
    entry += sizeof(uint64_t);
    auto& currVal = *reinterpret_cast<int64_t const*>(entry);
    entry += sizeof(int64_t) * 2;
    auto& prevVal = *reinterpret_cast<int64_t const*>(entry);
    sequenceEntry->replayVal(usageCount - 1, prevVal, currVal);
}

} // namespace storage
} // namespace kuzu
