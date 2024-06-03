#include "storage/stats/nodes_store_statistics.h"

#include "storage/storage_structure/disk_array_collection.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodesStoreStatsAndDeletedIDs::NodesStoreStatsAndDeletedIDs(const std::string& databasePath,
    DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal, VirtualFileSystem* fs,
    main::ClientContext* context)
    : TablesStatistics{metadataDAC, bufferManager, wal} {
    if (fs->fileOrPathExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(fs, databasePath,
                                 FileVersionType::ORIGINAL),
            context)) {
        readFromFile(databasePath, FileVersionType::ORIGINAL, fs, context);
    } else {
        saveToFile(databasePath, FileVersionType::ORIGINAL, TransactionType::READ_ONLY, fs);
    }
}

offset_t NodesStoreStatsAndDeletedIDs::getMaxNodeOffset(Transaction* transaction,
    table_id_t tableID) {
    KU_ASSERT(transaction);
    if (transaction->getType() == TransactionType::READ_ONLY) {
        return getNodeStatisticsAndDeletedIDs(transaction, tableID)->getMaxNodeOffset();
    } else {
        std::unique_lock xLck{mtx};
        return readWriteVersion == nullptr ?
                   getNodeStatisticsAndDeletedIDs(transaction, tableID)->getMaxNodeOffset() :
                   getNodeTableStats(TransactionType::WRITE, tableID)->getMaxNodeOffset();
    }
}

std::pair<offset_t, bool> NodesStoreStatsAndDeletedIDs::addNode(table_id_t tableID) {
    lock_t lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    return getNodeTableStats(transaction::TransactionType::WRITE, tableID)->addNode();
}

void NodesStoreStatsAndDeletedIDs::deleteNode(table_id_t tableID, offset_t nodeOffset) {
    lock_t lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    getNodeTableStats(transaction::TransactionType::WRITE, tableID)->deleteNode(nodeOffset);
}

void NodesStoreStatsAndDeletedIDs::updateNumTuplesByValue(table_id_t tableID, int64_t value) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    auto tableStats = getNodeTableStats(TransactionType::WRITE, tableID);
    tableStats->setNumTuples(tableStats->getNumTuples() + value);
}

void NodesStoreStatsAndDeletedIDs::addNodeStatisticsAndDeletedIDs(
    catalog::NodeTableCatalogEntry* nodeTableEntry) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion);
    setToUpdated();
    readWriteVersion->tableStatisticPerTable[nodeTableEntry->getTableID()] =
        constructTableStatistic(nodeTableEntry);
}

void NodesStoreStatsAndDeletedIDs::addMetadataDAHInfo(table_id_t tableID,
    const LogicalType& dataType) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion);
    setToUpdated();
    auto tableStats = dynamic_cast<NodeTableStatsAndDeletedIDs*>(
        readWriteVersion->tableStatisticPerTable[tableID].get());
    tableStats->addMetadataDAHInfoForColumn(createMetadataDAHInfo(dataType, metadataDAC));
}

void NodesStoreStatsAndDeletedIDs::removeMetadataDAHInfo(table_id_t tableID, column_id_t columnID) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion);
    setToUpdated();
    auto tableStats = dynamic_cast<NodeTableStatsAndDeletedIDs*>(
        readWriteVersion->tableStatisticPerTable[tableID].get());
    tableStats->removeMetadataDAHInfoForColumn(columnID);
}

MetadataDAHInfo* NodesStoreStatsAndDeletedIDs::getMetadataDAHInfo(Transaction* transaction,
    table_id_t tableID, column_id_t columnID) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    KU_ASSERT(transaction->isReadOnly() ||
              (transaction->isWriteTransaction() && readWriteVersion &&
                  readWriteVersion->tableStatisticPerTable.contains(tableID)));
    auto nodeTableStats = getNodeTableStats(transaction->getType(), tableID);
    return nodeTableStats->getMetadataDAHInfo(columnID);
}

} // namespace storage
} // namespace kuzu
