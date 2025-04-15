#include "storage/undo_buffer.h"

#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/table.h"
#include "storage/store/update_info.h"
#include "storage/store/version_record_handler.h"
#include "transaction/transaction.h"

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
    CatalogSet* catalogSet{};
    CatalogEntry* catalogEntry{};
    bool dropStorage{};
    // If this field is populated it stores the column to drop
    // Otherwise we will drop the storage for the whole entry (table/rel group)
    std::optional<column_id_t> droppedColumn;

    void dropStorageIfNeeded(StorageManager& storageManager) const;
};

struct SequenceEntryRecord {
    SequenceCatalogEntry* sequenceEntry;
    SequenceRollbackData sequenceRollbackData;
};

struct NodeBatchInsertRecord {
    table_id_t tableID;
};

struct VersionRecord {
    row_idx_t startRow;
    row_idx_t numRows;
    node_group_idx_t nodeGroupIdx;
    const VersionRecordHandler* versionRecordHandler;
};

struct VectorUpdateRecord {
    UpdateInfo* updateInfo;
    idx_t vectorIdx;
    VectorUpdateInfo* vectorUpdateInfo;
};

void CatalogEntryRecord::dropStorageIfNeeded(StorageManager& storageManager) const {
    if (!dropStorage) {
        return;
    }
    if (droppedColumn.has_value()) {
        KU_ASSERT(catalogEntry->getType() == catalog::CatalogEntryType::NODE_TABLE_ENTRY ||
                  catalogEntry->getType() == catalog::CatalogEntryType::REL_TABLE_ENTRY);
        storageManager.getTable(catalogEntry->constCast<TableCatalogEntry>().getTableID())
            ->commitDropColumn(*storageManager.getDataFH(), *droppedColumn);
    } else {
        if (catalogEntry->hasParent()) {
            return;
        }

        switch (catalogEntry->getType()) {
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_TABLE_ENTRY: {
            const auto& tableCatalogEntry = catalogEntry->constCast<TableCatalogEntry>();
            storageManager.getTable(tableCatalogEntry.getTableID())
                ->commitDrop(*storageManager.getDataFH());
        } break;
        case CatalogEntryType::REL_GROUP_ENTRY: {
            // Renaming tables also drops (then creates) the catalog entry, we don't want to
            // drop the storage table in that case
            const auto newCatalogEntry = catalogEntry->getNext();
            if (newCatalogEntry->getType() == CatalogEntryType::DUMMY_ENTRY) {
                const auto& relGroupCatalogEntry = catalogEntry->constCast<RelGroupCatalogEntry>();
                for (auto tableID : relGroupCatalogEntry.getRelTableIDs()) {
                    storageManager.getTable(tableID)->commitDrop(*storageManager.getDataFH());
                }
            }
        } break;
        default:
            break;
        }
    }
}

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

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) {
    createCatalogEntry(catalogSet, catalogEntry, catalogEntry.getNext()->isDeleted());
}

void UndoBuffer::createCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    bool dropStorage, std::optional<common::column_id_t> droppedColumn) {
    auto buffer = createUndoRecord<CatalogEntryRecord>(UndoRecordType::CATALOG_ENTRY);
    const CatalogEntryRecord catalogEntryRecord{&catalogSet, &catalogEntry, dropStorage,
        droppedColumn};
    *reinterpret_cast<CatalogEntryRecord*>(buffer) = catalogEntryRecord;
}

void UndoBuffer::createAlterCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    const binder::BoundAlterInfo& alterInfo) {
    switch (alterInfo.alterType) {
    case AlterType::DROP_PROPERTY: {
        const auto columnID = catalogEntry.constCast<TableCatalogEntry>().getColumnID(
            alterInfo.extraInfo->constCast<binder::BoundExtraDropPropertyInfo>().propertyName);
        createCatalogEntry(catalogSet, catalogEntry, true, columnID);
        break;
    }
    default: {
        createCatalogEntry(catalogSet, catalogEntry, false);
        break;
    }
    }
}

void UndoBuffer::createSequenceChange(SequenceCatalogEntry& sequenceEntry,
    const SequenceRollbackData& data) {
    auto buffer = createUndoRecord<SequenceEntryRecord>(UndoRecordType::SEQUENCE_ENTRY);
    const SequenceEntryRecord sequenceEntryRecord{&sequenceEntry, data};
    *reinterpret_cast<SequenceEntryRecord*>(buffer) = sequenceEntryRecord;
}

void UndoBuffer::createInsertInfo(node_group_idx_t nodeGroupIdx, row_idx_t startRow,
    row_idx_t numRows, const VersionRecordHandler* versionRecordHandler) {
    createVersionInfo(UndoRecordType::INSERT_INFO, startRow, numRows, versionRecordHandler,
        nodeGroupIdx);
}

void UndoBuffer::createDeleteInfo(node_group_idx_t nodeGroupIdx, row_idx_t startRow,
    row_idx_t numRows, const VersionRecordHandler* versionRecordHandler) {
    createVersionInfo(UndoRecordType::DELETE_INFO, startRow, numRows, versionRecordHandler,
        nodeGroupIdx);
}

void UndoBuffer::createVersionInfo(const UndoRecordType recordType, row_idx_t startRow,
    row_idx_t numRows, const VersionRecordHandler* versionRecordHandler,
    node_group_idx_t nodeGroupIdx) {
    KU_ASSERT(versionRecordHandler);
    auto buffer = createUndoRecord<VersionRecord>(recordType);
    *reinterpret_cast<VersionRecord*>(buffer) =
        VersionRecord{startRow, numRows, nodeGroupIdx, versionRecordHandler};
}

void UndoBuffer::createVectorUpdateInfo(UpdateInfo* updateInfo, const idx_t vectorIdx,
    VectorUpdateInfo* vectorUpdateInfo) {
    auto buffer = createUndoRecord<VectorUpdateRecord>(UndoRecordType::UPDATE_INFO);
    const VectorUpdateRecord vectorUpdateRecord{updateInfo, vectorIdx, vectorUpdateInfo};
    *reinterpret_cast<VectorUpdateRecord*>(buffer) = vectorUpdateRecord;
}

template<typename UndoEntry>
uint8_t* populateRecordHeader(uint8_t* buffer, UndoBuffer::UndoRecordType recordType) {
    const UndoRecordHeader recordHeader{recordType, sizeof(UndoEntry)};
    *reinterpret_cast<UndoRecordHeader*>(buffer) = recordHeader;
    return buffer + sizeof(UndoRecordHeader);
}

template<typename UndoEntry>
uint8_t* UndoBuffer::createUndoRecord(UndoRecordType recordType) {
    static constexpr auto size = sizeof(UndoRecordHeader) + sizeof(UndoEntry);
    std::unique_lock xLck{mtx};
    if (memoryBuffers.empty() || !memoryBuffers.back().canFit(size)) {
        auto capacity = UndoMemoryBuffer::UNDO_MEMORY_BUFFER_INIT_CAPACITY;
        while (size > capacity) {
            capacity *= 2;
        }
        // We need to allocate a new memory buffer.
        memoryBuffers.emplace_back(mm->allocateBuffer(false, capacity), capacity);
    }
    auto* res = memoryBuffers.back().getDataUnsafe() + memoryBuffers.back().getCurrentPosition();
    memoryBuffers.back().moveCurrentPosition(size);
    return populateRecordHeader<UndoEntry>(res, recordType);
}

void UndoBuffer::commit(transaction_t commitTS) const {
    UndoBufferIterator iterator{*this};
    iterator.iterate([&](UndoRecordType entryType, uint8_t const* entry) {
        commitRecord(entryType, entry, commitTS);
    });
}

void UndoBuffer::rollback(const transaction::Transaction* transaction) const {
    UndoBufferIterator iterator{*this};
    iterator.reverseIterate([&](UndoRecordType entryType, uint8_t const* entry) {
        rollbackRecord(transaction, entryType, entry);
    });
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
    const auto& catalogRecord = *reinterpret_cast<CatalogEntryRecord const*>(record);
    const auto newCatalogEntry = catalogRecord.catalogEntry->getNext();
    KU_ASSERT(newCatalogEntry);
    newCatalogEntry->setTimestamp(commitTS);
    catalogRecord.dropStorageIfNeeded(*ctx->getStorageManager());
}

void UndoBuffer::commitVersionInfo(UndoRecordType recordType, const uint8_t* record,
    transaction_t commitTS) {
    const auto& undoRecord = *reinterpret_cast<VersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        undoRecord.versionRecordHandler->applyFuncToChunkedGroups(&ChunkedNodeGroup::commitInsert,
            undoRecord.nodeGroupIdx, undoRecord.startRow, undoRecord.numRows, commitTS);
    } break;
    case UndoRecordType::DELETE_INFO: {
        undoRecord.versionRecordHandler->applyFuncToChunkedGroups(&ChunkedNodeGroup::commitDelete,
            undoRecord.nodeGroupIdx, undoRecord.startRow, undoRecord.numRows, commitTS);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::commitVectorUpdateInfo(const uint8_t* record, transaction_t commitTS) {
    auto& undoRecord = *reinterpret_cast<VectorUpdateRecord const*>(record);
    undoRecord.vectorUpdateInfo->version = commitTS;
}

void UndoBuffer::rollbackRecord(const transaction::Transaction* transaction,
    const UndoRecordType recordType, const uint8_t* record) {
    switch (recordType) {
    case UndoRecordType::CATALOG_ENTRY: {
        rollbackCatalogEntryRecord(record);
    } break;
    case UndoRecordType::SEQUENCE_ENTRY: {
        rollbackSequenceEntry(record);
    } break;
    case UndoRecordType::INSERT_INFO:
    case UndoRecordType::DELETE_INFO: {
        rollbackVersionInfo(transaction, recordType, record);
    } break;
    case UndoRecordType::UPDATE_INFO: {
        rollbackVectorUpdateInfo(transaction, record);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackCatalogEntryRecord(const uint8_t* record) {
    const auto& [catalogSet, catalogEntry, _1, _2] =
        *reinterpret_cast<CatalogEntryRecord const*>(record);
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
        catalogSet->eraseNoLock(catalogEntry->getName());
        if (olderEntry) {
            catalogSet->emplaceNoLock(std::move(olderEntry));
        }
    }
}

void UndoBuffer::commitSequenceEntry(const uint8_t*, transaction_t) {
    // DO NOTHING.
}

void UndoBuffer::rollbackSequenceEntry(const uint8_t* entry) {
    const auto& sequenceRecord = *reinterpret_cast<SequenceEntryRecord const*>(entry);
    const auto sequenceEntry = sequenceRecord.sequenceEntry;
    const auto& data = sequenceRecord.sequenceRollbackData;
    sequenceEntry->rollbackVal(data.usageCount, data.currVal);
}

void UndoBuffer::rollbackVersionInfo(const transaction::Transaction* transaction,
    UndoRecordType recordType, const uint8_t* record) {
    auto& undoRecord = *reinterpret_cast<VersionRecord const*>(record);
    switch (recordType) {
    case UndoRecordType::INSERT_INFO: {
        undoRecord.versionRecordHandler->rollbackInsert(transaction, undoRecord.nodeGroupIdx,
            undoRecord.startRow, undoRecord.numRows);
    } break;
    case UndoRecordType::DELETE_INFO: {
        undoRecord.versionRecordHandler->applyFuncToChunkedGroups(&ChunkedNodeGroup::rollbackDelete,
            undoRecord.nodeGroupIdx, undoRecord.startRow, undoRecord.numRows,
            transaction->getCommitTS());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void UndoBuffer::rollbackVectorUpdateInfo(const transaction::Transaction* transaction,
    const uint8_t* record) {
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
