#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodesStoreStatsAndDeletedIDs::NodesStoreStatsAndDeletedIDs(const std::string& databasePath,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, VirtualFileSystem* fs)
    : TablesStatistics{metadataFH, bufferManager, wal} {
    if (fs->fileOrPathExists(StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(fs, databasePath,
            FileVersionType::ORIGINAL))) {
        readFromFile(databasePath, FileVersionType::ORIGINAL, fs);
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

offset_t NodesStoreStatsAndDeletedIDs::addNode(table_id_t tableID) {
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

void NodesStoreStatsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(Transaction* tx,
    ValueVector* nodeIDVector, table_id_t tableID) {
    // NOTE: We can remove the lock under the following assumptions, that should currently hold:
    // 1) During the phases when nodeStatisticsAndDeletedIDsPerTableForReadOnlyTrx change, which
    // is during checkpointing, this function, which is called during scans, cannot be called.
    // 2) In a read-only transaction, the same morsel cannot be scanned concurrently. 3) A
    // write transaction cannot have two concurrent pipelines where one is writing and the
    // other is reading nodeStatisticsAndDeletedIDsPerTableForWriteTrx. That is the pipeline in a
    // query where scans/reads happen in a write transaction cannot run concurrently with the
    // pipeline that performs an add/delete node.
    lock_t lck{mtx};
    const TablesStatisticsContent* content;
    if (tx->isReadOnly() || readWriteVersion == nullptr) {
        content = getVersion(TransactionType::READ_ONLY);
    } else {
        content = getVersion(TransactionType::WRITE);
    }
    auto tableStat = content->getTableStat(tableID);
    auto nodeStat =
        ku_dynamic_cast<const TableStatistics*, const NodeTableStatsAndDeletedIDs*>(tableStat);
    nodeStat->setDeletedNodeOffsetsForMorsel(nodeIDVector);
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
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal));
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
