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

struct NodeBatchInsertRecord {
    table_id_t tableID;
};

struct VectorVersionRecord {
    VersionInfo* versionInfo;
    idx_t vectorIdx;
    VectorVersionInfo* vectorVersionInfo;
    std::vector<row_idx_t> rowsInVector;
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

void UndoBuffer::createNodeBatchInsert(table_id_t tableID) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(NodeBatchInsertRecord));
    const UndoRecordHeader recordHeader{UndoRecordType::NODE_BATCH_INSERT,
        sizeof(NodeBatchInsertRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const NodeBatchInsertRecord nodeBatchInsertRecord{tableID};
    *reinterpret_cast<NodeBatchInsertRecord*>(buffer) = nodeBatchInsertRecord;
}

void UndoBuffer::createVectorInsertInfo(VersionInfo* versionInfo, const idx_t vectorIdx,
    VectorVersionInfo* vectorVersionInfo, const std::vector<row_idx_t>& rowsInVector) {
    createVectorVersionInfo(UndoRecordType::INSERT_INFO, versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector);
}

void UndoBuffer::createVectorDeleteInfo(VersionInfo* versionInfo, const idx_t vectorIdx,
    VectorVersionInfo* vectorVersionInfo, const std::vector<row_idx_t>& rowsInVector) {
    createVectorVersionInfo(UndoRecordType::DELETE_INFO, versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector);
}

void UndoBuffer::createVectorVersionInfo(const UndoRecordType recordType, VersionInfo* versionInfo,
    const idx_t vectorIdx, VectorVersionInfo* vectorVersionInfo,
    const std::vector<row_idx_t>& rowsInVector) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VectorVersionRecord));
    const UndoRecordHeader recordHeader{recordType, sizeof(VectorVersionRecord)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    buffer += sizeof(UndoRecordHeader);
    const VectorVersionRecord vectorVersionRecord{versionInfo, vectorIdx, vectorVersionInfo,
        rowsInVector};
    *reinterpret_cast<VectorVersionRecord*>(buffer) = vectorVersionRecord;
}

void UndoBuffer::createVectorUpdateInfo(UpdateInfo* updateInfo, const idx_t vectorIdx,
    VectorUpdateInfo* vectorUpdateInfo) {
    auto buffer = createUndoRecord(sizeof(UndoRecordHeader) + sizeof(VectorUpdateInfo));
    const UndoRecordHeader recorHeader{UndoRecordType::UPDATE_INFO, sizeof(VectorUpdateInfo)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recorHeader;
    buffer += sizeof(UndoRecordHeader);
    const VectorUpdateRecord vectorUpdateRecord{updateInfo, vectorIdx, vectorUpdateInfo};
    *reinterpret_cast<VectorUpdateRecord*>(buffer) = vectorUpdateRecord;
}

uint8_t* UndoBuffer::createUndoRecord(const uint64_t size) {
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

void UndoBuffer::commitRecord(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) const {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        commitCatalogEntryRecord(record, commitTS);
    } break;
    case UndoRecordType::SEQUENCE_ENTRY: {
        commitSequenceEntry(record, commitTS);
    } break;
    case UndoRecordType::NODE_BATCH_INSERT: {
        commitNodeBatchInsertRecord(record, commitTS);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        commitVectorVersionInfo(recordType, record, commitTS);
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

void UndoBuffer::commitVectorVersionInfo(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) const {
    const auto& undoRecord = *reinterpret_cast<VectorVersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        for (const auto rowIdx : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->insertedVersions[rowIdx] = commitTS;
        }
    } break;
    case UndoRecordType::DELETE_INFO: {
        for (const auto rowIdx : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->deletedVersions[rowIdx] = commitTS;
        }
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

void UndoBuffer::commitNodeBatchInsertRecord(const uint8_t*, transaction_t) const {}

void UndoBuffer::rollbackRecord(const UndoRecordType recordType, const uint8_t* record) {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        rollbackCatalogEntryRecord(record);
    } break;
    case UndoRecordType::SEQUENCE_ENTRY: {
        rollbackSequenceEntry(record);
    }
    case UndoRecordType::NODE_BATCH_INSERT: {
        rollbackNodeBatchInsertRecord(record);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        rollbackVectorVersionInfo(recordType, record);
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

void UndoBuffer::rollbackNodeBatchInsertRecord(const uint8_t*) {
    // TODO(Guodong): Implement rollbackNodeBatchInsertRecord.
}

void UndoBuffer::rollbackVectorVersionInfo(UndoRecordType recordType, const uint8_t* record) {
    auto& undoRecord = *reinterpret_cast<VectorVersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        for (const auto row : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->insertedVersions[row] = INVALID_TRANSACTION;
        }
        // TODO(Guodong): Should handle hash index rollback.
        // TODO(Guodong): Refactor. Move these into VersionInfo.
        bool hasAnyInsertions = false;
        for (const auto& version : undoRecord.vectorVersionInfo->insertedVersions) {
            if (version != INVALID_TRANSACTION) {
                hasAnyInsertions = true;
                break;
            }
        }
        if (!hasAnyInsertions) {
            if (undoRecord.vectorVersionInfo->deletionStatus ==
                VectorVersionInfo::DeletionStatus::NO_DELETED) {
                undoRecord.versionInfo->clearVectorInfo(undoRecord.vectorIdx);
            } else {
                undoRecord.vectorVersionInfo->insertionStatus =
                    VectorVersionInfo::InsertionStatus::NO_INSERTED;
            }
        }
    } break;
    case UndoRecordType::DELETE_INFO: {
        for (const auto row : undoRecord.rowsInVector) {
            undoRecord.vectorVersionInfo->deletedVersions[row] = INVALID_TRANSACTION;
        }
        // TODO(Guodong): Refactor. Move these into VersionInfo.
        bool hasAnyDeletions = false;
        for (const auto& version : undoRecord.vectorVersionInfo->deletedVersions) {
            if (version != INVALID_TRANSACTION) {
                hasAnyDeletions = true;
                break;
            }
        }
        if (!hasAnyDeletions) {
            if (undoRecord.vectorVersionInfo->insertionStatus ==
                VectorVersionInfo::InsertionStatus::NO_INSERTED) {
                undoRecord.versionInfo->clearVectorInfo(undoRecord.vectorIdx);
            } else {
                undoRecord.vectorVersionInfo->deletionStatus =
                    VectorVersionInfo::DeletionStatus::NO_DELETED;
            }
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackVectorUpdateInfo(const uint8_t* record) const {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    KU_ASSERT(undoRecord.updateInfo);
    if (undoRecord.updateInfo->getVectorInfo(clientContext.getTx(), undoRecord.vectorIdx) !=
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
