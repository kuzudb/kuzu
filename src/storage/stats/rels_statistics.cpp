#include "storage/stats/rels_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void RelTableStats::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(nextRelOffset, fileInfo, offset);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(
    uint64_t numRels, common::table_id_t tableID, FileInfo* fileInfo, uint64_t& offset) {
    common::offset_t nextRelOffset;
    SerDeser::deserializeValue(nextRelOffset, fileInfo, offset);
    return std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
}

// We should only call this function after we call setNumRelsPerDirectionBoundTableID.
void RelsStatistics::setNumTuplesForTable(table_id_t relTableID, uint64_t numRels) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(relTableID));
    auto relStatistics =
        (RelTableStats*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    increaseNextRelOffset(relTableID, numRels - relStatistics->getNumTuples());
    relStatistics->setNumTuples(numRels);
}

void RelsStatistics::updateNumRelsByValue(table_id_t relTableID, int64_t value) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    auto relStatistics =
        (RelTableStats*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
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
    std::unique_lock lck{mtx};
    auto& tableStatisticContent =
        (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
            tablesStatisticsContentForReadOnlyTrx :
            tablesStatisticsContentForWriteTrx;
    return ((RelTableStats*)tableStatisticContent->tableStatisticPerTable.at(tableID).get())
        ->getNextRelOffset();
}

} // namespace storage
} // namespace kuzu
