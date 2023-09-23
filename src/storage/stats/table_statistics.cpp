#include "storage/stats/table_statistics.h"

#include "catalog/table_schema.h"
#include "common/ser_deser.h"
#include "storage/stats/node_table_statistics.h"
#include "storage/stats/rel_table_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TableStatistics::TableStatistics(const catalog::TableSchema& schema)
    : tableType{schema.tableType}, numTuples{0}, tableID{schema.tableID} {
    for (auto property : schema.getProperties()) {
        propertyStatistics[property->getPropertyID()] = std::make_unique<PropertyStatistics>();
    }
}

TableStatistics::TableStatistics(common::TableType tableType, uint64_t numTuples,
    common::table_id_t tableID,
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
        propertyStatistics)
    : tableType{tableType}, numTuples{numTuples}, tableID{tableID}, propertyStatistics{std::move(
                                                                        propertyStatistics)} {
    assert(numTuples != UINT64_MAX);
}

TableStatistics::TableStatistics(const TableStatistics& other)
    : tableType{other.tableType}, numTuples{other.numTuples}, tableID{other.tableID} {
    for (auto& propertyStats : other.propertyStatistics) {
        propertyStatistics[propertyStats.first] =
            std::make_unique<PropertyStatistics>(*propertyStats.second.get());
    }
}

void TableStatistics::serialize(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(tableType, fileInfo, offset);
    SerDeser::serializeValue(numTuples, fileInfo, offset);
    SerDeser::serializeValue(tableID, fileInfo, offset);
    SerDeser::serializeUnorderedMap(propertyStatistics, fileInfo, offset);
    serializeInternal(fileInfo, offset);
}

std::unique_ptr<TableStatistics> TableStatistics::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    TableType tableType;
    uint64_t numTuples;
    table_id_t tableID;
    std::unordered_map<property_id_t, std::unique_ptr<PropertyStatistics>> propertyStatistics;
    SerDeser::deserializeValue(tableType, fileInfo, offset);
    SerDeser::deserializeValue(numTuples, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
    SerDeser::deserializeUnorderedMap(propertyStatistics, fileInfo, offset);
    std::unique_ptr<TableStatistics> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableStatsAndDeletedIDs::deserialize(tableID,
            NodeTableStatsAndDeletedIDs::getMaxNodeOffsetFromNumTuples(numTuples), fileInfo,
            offset);
    } break;
    case TableType::REL: {
        result = RelTableStats::deserialize(numTuples, tableID, fileInfo, offset);
    } break;
    // LCOV_EXCL_START
    default: {
        throw NotImplementedException("TableStatistics::deserialize");
    }
        // LCOV_EXCL_STOP
    }
    result->tableType = tableType;
    result->numTuples = numTuples;
    result->tableID = tableID;
    result->propertyStatistics = std::move(propertyStatistics);
    return result;
}

} // namespace storage
} // namespace kuzu
