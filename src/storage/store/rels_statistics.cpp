#include "src/storage/store/include/rels_statistics.h"

namespace graphflow {
namespace storage {

RelStatistics::RelStatistics(SrcDstTableIDs srcDstTableIDs) : TableStatistics{0} {
    numRelsPerDirectionBoundTable.resize(2);
    for (auto srcTableID : srcDstTableIDs.srcTableIDs) {
        numRelsPerDirectionBoundTable[RelDirection::FWD].emplace(srcTableID, 0);
    }
    for (auto dstTableID : srcDstTableIDs.dstTableIDs) {
        numRelsPerDirectionBoundTable[RelDirection::BWD].emplace(dstTableID, 0);
    }
}

RelsStatistics::RelsStatistics(
    unordered_map<table_id_t, unique_ptr<RelStatistics>> relStatisticPerTable_)
    : TablesStatistics{} {
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto& relStatistic : relStatisticPerTable_) {
        (*tableStatisticPerTableForReadOnlyTrx)[relStatistic.first] =
            make_unique<RelStatistics>(*(RelStatistics*)relStatistic.second.get());
        (*tableStatisticPerTableForWriteTrx)[relStatistic.first] =
            make_unique<RelStatistics>(*(RelStatistics*)relStatistic.second.get());
    }
}

void RelsStatistics::setNumRelsPerDirectionBoundTableID(
    table_id_t tableID, vector<map<table_id_t, atomic<uint64_t>>>& directionNumRelsPerTable) {
    lock_t lck{mtx};
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto const& tableIDNumRelPair : directionNumRelsPerTable[relDirection]) {
            auto boundTableID = tableIDNumRelPair.first;
            ((RelStatistics*)((*tableStatisticPerTableForWriteTrx)[tableID].get()))
                ->setNumRelsForDirectionBoundTable(
                    relDirection, boundTableID, tableIDNumRelPair.second.load());
        }
    }
}

uint64_t RelsStatistics::getNextRelID() {
    auto nextRelID = 0ull;
    for (auto& tableStatistic : *tableStatisticPerTableForReadOnlyTrx) {
        nextRelID += ((RelStatistics*)tableStatistic.second.get())->getNumTuples();
    }
    return nextRelID;
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
} // namespace graphflow
