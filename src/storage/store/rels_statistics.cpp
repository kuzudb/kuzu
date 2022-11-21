#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace storage {

RelStatistics::RelStatistics(vector<pair<table_id_t, table_id_t>> srcDstTableIDs)
    : TableStatistics{0} {
    numRelsPerDirectionBoundTable.resize(2);
    for (auto& [srcTableID, dstTableID] : srcDstTableIDs) {
        numRelsPerDirectionBoundTable[RelDirection::FWD].emplace(srcTableID, 0);
        numRelsPerDirectionBoundTable[RelDirection::BWD].emplace(dstTableID, 0);
    }
}

RelsStatistics::RelsStatistics(
    unordered_map<table_id_t, unique_ptr<RelStatistics>> relStatisticPerTable_)
    : TablesStatistics{} {
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto& relStatistic : relStatisticPerTable_) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[relStatistic.first] =
            make_unique<RelStatistics>(*(RelStatistics*)relStatistic.second.get());
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relStatistic.first] =
            make_unique<RelStatistics>(*(RelStatistics*)relStatistic.second.get());
    }
}

// We should only call this function after we call setNumRelsPerDirectionBoundTableID.
void RelsStatistics::setNumRelsForTable(table_id_t relTableID, uint64_t numRels) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(relTableID));
    auto relStatistics =
        (RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[relTableID]
            .get();
    tablesStatisticsContentForWriteTrx->nextRelID += (numRels - relStatistics->getNumTuples());
    relStatistics->setNumTuples(numRels);
    assertNumRelsIsSound(relStatistics->numRelsPerDirectionBoundTable[FWD], numRels);
    assertNumRelsIsSound(relStatistics->numRelsPerDirectionBoundTable[BWD], numRels);
}

void RelsStatistics::assertNumRelsIsSound(
    unordered_map<table_id_t, uint64_t>& relsPerBoundTable, uint64_t numRels) {
    uint64_t sum = 0;
    for (auto tableIDNumRels : relsPerBoundTable) {
        sum += tableIDNumRels.second;
    }
    assert(sum == numRels);
}

// We should only call this function after we call incrementNumRelsPerDirectionBoundTableByOne.
void RelsStatistics::incrementNumRelsByOneForTable(table_id_t relTableID) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    lck.unlock();
    setNumRelsForTable(relTableID,
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(relTableID)->getNumTuples() +
            1);
}

void RelsStatistics::incrementNumRelsPerDirectionBoundTableByOne(
    table_id_t relTableID, table_id_t srcTableID, table_id_t dstTableID) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto relDirection : REL_DIRECTIONS) {
        auto relStatistics =
            (RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable
                .at(relTableID)
                .get();
        relStatistics->numRelsPerDirectionBoundTable[relDirection].at(
            relDirection == FWD ? srcTableID : dstTableID)++;
    }
}

void RelsStatistics::setNumRelsPerDirectionBoundTableID(
    table_id_t tableID, vector<map<table_id_t, atomic<uint64_t>>>& directionNumRelsPerTable) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto const& tableIDNumRelPair : directionNumRelsPerTable[relDirection]) {
            ((RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID]
                    .get())
                ->setNumRelsForDirectionBoundTable(
                    relDirection, tableIDNumRelPair.first, tableIDNumRelPair.second.load());
        }
    }
}

uint64_t RelsStatistics::getNextRelID(Transaction* transaction) {
    lock_t lck{mtx};
    auto& tableStatisticContent =
        (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
            tablesStatisticsContentForReadOnlyTrx :
            tablesStatisticsContentForWriteTrx;
    return tableStatisticContent->nextRelID;
}

unique_ptr<TableStatistics> RelsStatistics::deserializeTableStatistics(
    uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) {
    vector<unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTable{2};
    offset = SerDeser::deserializeUnorderedMap(numRelsPerDirectionBoundTable[0], fileInfo, offset);
    offset = SerDeser::deserializeUnorderedMap(numRelsPerDirectionBoundTable[1], fileInfo, offset);
    return make_unique<RelStatistics>(numTuples, move(numRelsPerDirectionBoundTable));
}

void RelsStatistics::serializeTableStatistics(
    TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) {
    auto relStatistic = (RelStatistics*)tableStatistics;
    offset = SerDeser::serializeUnorderedMap(
        relStatistic->numRelsPerDirectionBoundTable[0], fileInfo, offset);
    offset = SerDeser::serializeUnorderedMap(
        relStatistic->numRelsPerDirectionBoundTable[1], fileInfo, offset);
}

} // namespace storage
} // namespace kuzu
