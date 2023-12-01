#include "storage/stats/rels_store_statistics.h"

#include "common/assert.h"
#include "storage/stats/rel_table_statistics.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelsStoreStats::RelsStoreStats(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal)
    : TablesStatistics{metadataFH, bufferManager, wal} {
    readFromFile(wal->getDirectory());
}

// We should only call this function after we call setNumRelsPerDirectionBoundTableID.
void RelsStoreStats::setNumTuplesForTable(table_id_t relTableID, uint64_t numRels) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    KU_ASSERT(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(relTableID));
    auto relStatistics =
        (RelTableStats*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    increaseNextRelOffset(relTableID, numRels - relStatistics->getNumTuples());
    relStatistics->setNumTuples(numRels);
}

void RelsStoreStats::updateNumRelsByValue(table_id_t relTableID, int64_t value) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    auto relStatistics =
        (RelTableStats*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    auto numRelsBeforeUpdate = relStatistics->getNumTuples();
    KU_ASSERT(!(numRelsBeforeUpdate == 0 && value < 0));
    auto numRelsAfterUpdate = relStatistics->getNumTuples() + value;
    relStatistics->setNumTuples(numRelsAfterUpdate);
    // Update the nextRelID only when we are inserting rels.
    if (value > 0) {
        increaseNextRelOffset(relTableID, value);
    }
}

offset_t RelsStoreStats::getNextRelOffset(
    transaction::Transaction* transaction, table_id_t tableID) {
    std::unique_lock lck{mtx};
    auto& tableStatisticContent =
        (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
            tablesStatisticsContentForReadOnlyTrx :
            tablesStatisticsContentForWriteTrx;
    return ((RelTableStats*)tableStatisticContent->tableStatisticPerTable.at(tableID).get())
        ->getNextRelOffset();
}

void RelsStoreStats::addMetadataDAHInfo(table_id_t tableID, const LogicalType& dataType) {
    initTableStatisticsForWriteTrx();
    auto tableStats = dynamic_cast<RelTableStats*>(
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID].get());
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal), RelDataDirection::FWD);
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal), RelDataDirection::BWD);
}

void RelsStoreStats::removeMetadataDAHInfo(table_id_t tableID, column_id_t columnID) {
    initTableStatisticsForWriteTrx();
    auto tableStats = dynamic_cast<RelTableStats*>(
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID].get());
    tableStats->removeMetadataDAHInfoForColumn(columnID, RelDataDirection::FWD);
    tableStats->removeMetadataDAHInfoForColumn(columnID, RelDataDirection::BWD);
}

MetadataDAHInfo* RelsStoreStats::getCSROffsetMetadataDAHInfo(
    Transaction* transaction, table_id_t tableID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto tableStats = getRelStatistics(tableID, transaction);
    return tableStats->getCSROffsetMetadataDAHInfo(direction);
}

MetadataDAHInfo* RelsStoreStats::getAdjMetadataDAHInfo(
    Transaction* transaction, table_id_t tableID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto tableStats = getRelStatistics(tableID, transaction);
    return tableStats->getAdjMetadataDAHInfo(direction);
}

MetadataDAHInfo* RelsStoreStats::getPropertyMetadataDAHInfo(transaction::Transaction* transaction,
    table_id_t tableID, column_id_t columnID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto relTableStats = getRelStatistics(tableID, transaction);
    return relTableStats->getPropertyMetadataDAHInfo(columnID, direction);
}

} // namespace storage
} // namespace kuzu
