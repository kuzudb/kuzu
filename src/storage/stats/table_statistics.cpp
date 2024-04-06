#include "storage/stats/table_statistics.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/stats/node_table_statistics.h"
#include "storage/stats/rel_table_statistics.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TableStatistics::TableStatistics(const catalog::TableCatalogEntry& tableEntry)
    : tableType{tableEntry.getTableType()}, numTuples{0}, tableID{tableEntry.getTableID()} {
    for (auto& property : tableEntry.getPropertiesRef()) {
        propertyStatistics[property.getPropertyID()] = std::make_unique<PropertyStatistics>();
    }
}

TableStatistics::TableStatistics(common::TableType tableType, uint64_t numTuples,
    common::table_id_t tableID,
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
        propertyStatistics)
    : tableType{tableType}, numTuples{numTuples}, tableID{tableID},
      propertyStatistics{std::move(propertyStatistics)} {
    KU_ASSERT(numTuples != UINT64_MAX);
}

TableStatistics::TableStatistics(const TableStatistics& other)
    : tableType{other.tableType}, numTuples{other.numTuples}, tableID{other.tableID} {
    for (auto& propertyStats : other.propertyStatistics) {
        propertyStatistics[propertyStats.first] =
            std::make_unique<PropertyStatistics>(*propertyStats.second.get());
    }
}

void TableStatistics::serialize(Serializer& serializer) {
    serializer.serializeValue(tableType);
    serializer.serializeValue(numTuples);
    serializer.serializeValue(tableID);
    serializer.serializeUnorderedMap(propertyStatistics);
    serializeInternal(serializer);
}

std::unique_ptr<TableStatistics> TableStatistics::deserialize(Deserializer& deserializer) {
    TableType tableType;
    uint64_t numTuples;
    table_id_t tableID;
    std::unordered_map<property_id_t, std::unique_ptr<PropertyStatistics>> propertyStatistics;
    deserializer.deserializeValue(tableType);
    deserializer.deserializeValue(numTuples);
    deserializer.deserializeValue(tableID);
    deserializer.deserializeUnorderedMap(propertyStatistics);
    std::unique_ptr<TableStatistics> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableStatsAndDeletedIDs::deserialize(tableID,
            NodeTableStatsAndDeletedIDs::getMaxNodeOffsetFromNumTuples(numTuples), deserializer);
    } break;
    case TableType::REL: {
        result = RelTableStats::deserialize(numTuples, tableID, deserializer);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    result->tableType = tableType;
    result->numTuples = numTuples;
    result->tableID = tableID;
    result->propertyStatistics = std::move(propertyStatistics);
    return result;
}

} // namespace storage
} // namespace kuzu
