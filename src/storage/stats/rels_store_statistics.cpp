#include "storage/stats/rels_store_statistics.h"

#include "common/assert.h"
#include "storage/stats/rel_table_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// We should only call this function after we call setNumRelsPerDirectionBoundTableID.
void RelsStoreStats::setNumTuplesForTable(table_id_t relTableID, uint64_t numRels) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(relTableID));
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

} // namespace storage
} // namespace kuzu
