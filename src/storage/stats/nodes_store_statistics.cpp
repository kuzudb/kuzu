#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

offset_t NodesStoreStatsAndDeletedIDs::getMaxNodeOffset(
    transaction::Transaction* transaction, table_id_t tableID) {
    KU_ASSERT(transaction);
    if (transaction->getType() == transaction::TransactionType::READ_ONLY) {
        return getNodeStatisticsAndDeletedIDs(tableID)->getMaxNodeOffset();
    } else {
        std::unique_lock xLck{mtx};
        return tablesStatisticsContentForWriteTrx == nullptr ?
                   getNodeStatisticsAndDeletedIDs(tableID)->getMaxNodeOffset() :
                   getNodeTableStats(transaction::TransactionType::WRITE, tableID)
                       ->getMaxNodeOffset();
    }
}

std::map<table_id_t, offset_t> NodesStoreStatsAndDeletedIDs::getMaxNodeOffsetPerTable() const {
    std::map<table_id_t, offset_t> retVal;
    for (auto& tableIDStatistics : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
        retVal[tableIDStatistics.first] =
            getNodeStatisticsAndDeletedIDs(tableIDStatistics.first)->getMaxNodeOffset();
    }
    return retVal;
}

void NodesStoreStatsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(
    transaction::Transaction* transaction, const std::shared_ptr<ValueVector>& nodeOffsetVector,
    table_id_t tableID) {
    // NOTE: We can remove the lock under the following assumptions, that should currently hold:
    // 1) During the phases when nodeStatisticsAndDeletedIDsPerTableForReadOnlyTrx change, which
    // is during checkpointing, this function, which is called during scans, cannot be called.
    // 2) In a read-only transaction, the same morsel cannot be scanned concurrently. 3) A
    // write transaction cannot have two concurrent pipelines where one is writing and the
    // other is reading nodeStatisticsAndDeletedIDsPerTableForWriteTrx. That is the pipeline in a
    // query where scans/reads happen in a write transaction cannot run concurrently with the
    // pipeline that performs an add/delete node.
    lock_t lck{mtx};
    (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
        getNodeStatisticsAndDeletedIDs(tableID)->setDeletedNodeOffsetsForMorsel(nodeOffsetVector) :
        ((NodeTableStatsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
}

void NodesStoreStatsAndDeletedIDs::addNodeStatisticsAndDeletedIDs(
    catalog::NodeTableSchema* tableSchema) {
    initTableStatisticsForWriteTrx();
    tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
        constructTableStatistic(tableSchema);
}

void NodesStoreStatsAndDeletedIDs::addMetadataDAHInfo(
    table_id_t tableID, const LogicalType& dataType) {
    initTableStatisticsForWriteTrx();
    auto tableStats = dynamic_cast<NodeTableStatsAndDeletedIDs*>(
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID].get());
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal));
}

void NodesStoreStatsAndDeletedIDs::removeMetadataDAHInfo(table_id_t tableID, column_id_t columnID) {
    initTableStatisticsForWriteTrx();
    auto tableStats = dynamic_cast<NodeTableStatsAndDeletedIDs*>(
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID].get());
    tableStats->removeMetadataDAHInfoForColumn(columnID);
}

MetadataDAHInfo* NodesStoreStatsAndDeletedIDs::getMetadataDAHInfo(
    transaction::Transaction* transaction, table_id_t tableID, column_id_t columnID) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto nodeTableStats = getNodeTableStats(transaction->getType(), tableID);
    return nodeTableStats->getMetadataDAHInfo(columnID);
}

} // namespace storage
} // namespace kuzu
