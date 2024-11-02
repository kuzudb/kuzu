#include "storage/undo_buffer.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/update_info.h"

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
    SequenceRollbackData sequenceRollbackData;
};

struct NodeBatchInsertRecord {
    table_id_t tableID;
};

struct VersionRecord {
    ChunkedNodeGroup* chunkedNodeGroup;
    row_idx_t startRow;
    row_idx_t numRows;
};

struct VectorUpdateRecord {
    UpdateInfo* updateInfo;
    idx_t vectorIdx;
    VectorUpdateInfo* vectorUpdateInfo;
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

UndoBuffer::UndoBuffer(transaction::Transaction* transaction) : transaction{transaction} {}

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(CatalogEntryRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::CATALOG_ENTRY, sizeof(CatalogEntryRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const CatalogEntryRecord catalogEntryRecord{&catalogSet, &catalogEntry};
    *reinterpret_cast<CatalogEntryRecord*>(buffer) = catalogEntryRecord;
}

void UndoBuffer::createSequenceChange(SequenceCatalogEntry& sequenceEntry,
    const SequenceRollbackData& data) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(SequenceEntryRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::SEQUENCE_ENTRY,
        sizeof(SequenceEntryRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const SequenceEntryRecord sequenceEntryRecord{&sequenceEntry, data};
    *reinterpret_cast<SequenceEntryRecord*>(buffer) = sequenceEntryRecord;
}

void UndoBuffer::createInsertInfo(ChunkedNodeGroup* chunkedNodeGroup, row_idx_t startRow,
    row_idx_t numRows) {
    createVersionInfo(UndoRecordType::INSERT_INFO, chunkedNodeGroup, startRow, numRows);
}

void UndoBuffer::createDeleteInfo(ChunkedNodeGroup* chunkedNodeGroup, row_idx_t startRow,
    row_idx_t numRows) {
    createVersionInfo(UndoRecordType::DELETE_INFO, chunkedNodeGroup, startRow, numRows);
}

void UndoBuffer::createVersionInfo(const UndoRecordType recordType,
    ChunkedNodeGroup* chunkedNodeGroup, row_idx_t startRow, row_idx_t numRows) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VersionRecord));
    const UndoRecordHeader recordHeader{recordType, sizeof(VersionRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const VersionRecord vectorVersionRecord{chunkedNodeGroup, startRow, numRows};
    *reinterpret_cast<VersionRecord*>(buffer) = vectorVersionRecord;
}

void UndoBuffer::createVectorUpdateInfo(UpdateInfo* updateInfo, const idx_t vectorIdx,
    VectorUpdateInfo* vectorUpdateInfo) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VectorUpdateRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::UPDATE_INFO, sizeof(VectorUpdateRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const VectorUpdateRecord vectorUpdateRecord{updateInfo, vectorIdx, vectorUpdateInfo};
    *reinterpret_cast<VectorUpdateRecord*>(buffer) = vectorUpdateRecord;
}

uint8_t* UndoBuffer::createUndoRecord(const uint64_t size) {
    std::unique_lock xLck{mtx};
    if (memoryBuffers.empty() || !memoryBuffers.back().canFit(size)) {
        auto capacity = UndoMemoryBuffer::UNDO_MEMORY_BUFFER_SIZE;
        while (size > capacity) {
            capacity *= 2;
        }
        // We need to allocate a new memory buffer.
        memoryBuffers.push_back(UndoMemoryBuffer(capacity));
    }
    const auto res =
        memoryBuffers.back().getDataUnsafe() + memoryBuffers.back().getCurrentPosition();
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
        [&](UndoRecordType entryType, uint8_t const* entry) { rollbackRecord(entryType, entry); });
}

uint64_t UndoBuffer::getMemUsage() const {
    uint64_t totalMemUsage = 0;
    for (const auto& buffer : memoryBuffers) {
        totalMemUsage += buffer.getSize();
    }
    return totalMemUsage;
}

void UndoBuffer::commitRecord(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) const {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        commitCatalogEntryRecord(record, commitTS);
    } break;
    case UndoRecordType::SEQUENCE_ENTRY: {
        commitSequenceEntry(record, commitTS);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        commitVersionInfo(recordType, record, commitTS);
    } break;
    case UndoRecordType::UPDATE_INFO: {
        commitVectorUpdateInfo(record, commitTS);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void UndoBuffer::commitCatalogEntryRecord(const uint8_t* record,
    const transaction_t commitTS) const {
    const auto& [_, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto newCatalogEntry = catalogEntry->getNext();
    KU_ASSERT(newCatalogEntry);
    newCatalogEntry->setTimestamp(commitTS);
}

void UndoBuffer::commitVersionInfo(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) const {
    const auto& undoRecord = *reinterpret_cast<VersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        undoRecord.chunkedNodeGroup->commitInsert(undoRecord.startRow, undoRecord.numRows,
            commitTS);
    } break;
    case UndoRecordType::DELETE_INFO: {
        undoRecord.chunkedNodeGroup->commitDelete(undoRecord.startRow, undoRecord.numRows,
            commitTS);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::commitVectorUpdateInfo(const uint8_t* record, transaction_t commitTS) const {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    undoRecord.vectorUpdateInfo->version = commitTS;
}

void UndoBuffer::rollbackRecord(const UndoRecordType recordType, const uint8_t* record) {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        rollbackCatalogEntryRecord(record);
    } break;
    case UndoRecordType::SEQUENCE_ENTRY: {
        rollbackSequenceEntry(record);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        rollbackVersionInfo(recordType, record);
    } break;
    case UndoRecordType::UPDATE_INFO: {
        rollbackVectorUpdateInfo(record);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackCatalogEntryRecord(const uint8_t* record) {
    const auto& [catalogSet, catalogEntry] = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto entryToRollback = catalogEntry->getNext();
    KU_ASSERT(entryToRollback);
    const auto entryType = entryToRollback->getType();
    if (entryType == CatalogEntryType::NODE_TABLE_ENTRY ||
        entryType == CatalogEntryType::REL_TABLE_ENTRY ||
        entryType == CatalogEntryType::REL_GROUP_ENTRY) {
        entryToRollback->ptrCast<TableCatalogEntry>()->resetAlterInfo();
    }
    if (entryToRollback->getNext()) {
        // If entryToRollback has a newer entry (next) in the version chain. Simple remove
        // entryToRollback from the chain.
        const auto newerEntry = entryToRollback->getNext();
        newerEntry->setPrev(entryToRollback->movePrev());
    } else {
        // This is the begin of the version chain.
        auto olderEntry = entryToRollback->movePrev();
        catalogSet->eraseNoLock(catalogEntry->getName());
        if (olderEntry) {
            catalogSet->emplaceNoLock(std::move(olderEntry));
        }
    }
}

void UndoBuffer::commitSequenceEntry(const uint8_t*, transaction_t) const {
    // DO NOTHING.
}

void UndoBuffer::rollbackSequenceEntry(const uint8_t* entry) {
    const auto& sequenceRecord = *reinterpret_cast<SequenceEntryRecord const*>(entry);
    const auto sequenceEntry = sequenceRecord.sequenceEntry;
    const auto& data = sequenceRecord.sequenceRollbackData;
    sequenceEntry->rollbackVal(data.usageCount, data.currVal);
}

void UndoBuffer::rollbackVersionInfo(UndoRecordType recordType, const uint8_t* record) {
    auto& undoRecord = *reinterpret_cast<VersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        undoRecord.chunkedNodeGroup->rollbackInsert(undoRecord.startRow, undoRecord.numRows);
    } break;
    case UndoRecordType::DELETE_INFO: {
        undoRecord.chunkedNodeGroup->rollbackDelete(undoRecord.startRow, undoRecord.numRows);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackVectorUpdateInfo(const uint8_t* record) const {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    KU_ASSERT(undoRecord.updateInfo);
    if (undoRecord.updateInfo->getVectorInfo(transaction, undoRecord.vectorIdx) !=
        undoRecord.vectorUpdateInfo) {
        // The version chain has been updated. No need to rollback.
        return;
    }
    if (undoRecord.vectorUpdateInfo->getNext()) {
        // Has newer versions. Simply remove the current one from the version chain.
        const auto newerVersion = undoRecord.vectorUpdateInfo->getNext();
        auto prevVersion = undoRecord.vectorUpdateInfo->movePrev();
        prevVersion->next = newerVersion;
        newerVersion->setPrev(std::move(prevVersion));
    } else {
        // This is the begin of the version chain.
        if (auto prevVersion = undoRecord.vectorUpdateInfo->movePrev()) {
            undoRecord.updateInfo->setVectorInfo(undoRecord.vectorIdx, std::move(prevVersion));
        } else {
            undoRecord.updateInfo->clearVectorInfo(undoRecord.vectorIdx);
        }
    }
}

} // namespace storage
} // namespace kuzu
