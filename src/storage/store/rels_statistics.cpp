#include "storage/store/rels_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// We should only call this function after we call setNumRelsPerDirectionBoundTableID.
void RelsStatistics::setNumTuplesForTable(table_id_t relTableID, uint64_t numRels) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(relTableID));
    auto relStatistics =
        (RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    increaseNextRelOffset(relTableID, numRels - relStatistics->getNumTuples());
    relStatistics->setNumTuples(numRels);
}

void RelsStatistics::updateNumRelsByValue(table_id_t relTableID, int64_t value) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    auto relStatistics =
        (RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    auto numRelsAfterUpdate = relStatistics->getNumTuples() + value;
    relStatistics->setNumTuples(numRelsAfterUpdate);
    // Update the nextRelID only when we are inserting rels.
    if (value > 0) {
        increaseNextRelOffset(relTableID, value);
    }
}

offset_t RelsStatistics::getNextRelOffset(
    transaction::Transaction* transaction, table_id_t tableID) {
    lock_t lck{mtx};
    auto& tableStatisticContent =
        (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
            tablesStatisticsContentForReadOnlyTrx :
            tablesStatisticsContentForWriteTrx;
    return ((RelStatistics*)tableStatisticContent->tableStatisticPerTable.at(tableID).get())
        ->getNextRelOffset();
}

std::unique_ptr<TableStatistics> RelsStatistics::deserializeTableStatistics(
    uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) {
    std::vector<std::unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTable{2};
    offset_t nextRelOffset;
    SerDeser::deserializeValue(nextRelOffset, fileInfo, offset);
    return std::make_unique<RelStatistics>(numTuples, nextRelOffset);
}

void RelsStatistics::serializeTableStatistics(
    TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) {
    auto relStatistic = (RelStatistics*)tableStatistics;
    SerDeser::serializeValue(relStatistic->nextRelOffset, fileInfo, offset);
}

} // namespace storage
} // namespace kuzu
