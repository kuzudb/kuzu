#include "storage/stats/property_statistics.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/stats/table_statistics_collection.h"

namespace kuzu {
namespace storage {

void PropertyStatistics::serialize(common::Serializer& serializer) {
    serializer.serializeValue(mayHaveNullValue);
}

RWPropertyStats::RWPropertyStats(TablesStatistics* tablesStatistics, common::table_id_t tableID,
    common::property_id_t propertyID)
    : tablesStatistics{tablesStatistics}, tableID{tableID}, propertyID{propertyID} {}

std::unique_ptr<PropertyStatistics> PropertyStatistics::deserialize(
    common::Deserializer& deserializer) {
    bool hasNull;
    deserializer.deserializeValue<bool>(hasNull);
    return std::make_unique<PropertyStatistics>(hasNull);
}

// Read/write statistics cannot be cached since functions like checkpointInMemoryIfNecessary may
// overwrite them and invalidate the reference
bool RWPropertyStats::mayHaveNull(const transaction::Transaction& transaction) {
    // TODO(Guodong): INVALID_PROPERTY_ID is used here because we have some columns not exposed as
    // property in table schema. Should be fixed once we properly align properties and columns.
    if (propertyID == common::INVALID_PROPERTY_ID) {
        return true;
    }
    auto statistics =
        tablesStatistics->getPropertyStatisticsForTable(transaction, tableID, propertyID);
    return statistics.mayHaveNull();
}

void RWPropertyStats::setHasNull(const transaction::Transaction& transaction) {
    // TODO(Guodong): INVALID_PROPERTY_ID is used here because we have some columns not exposed as
    // property in table schema. Should be fixed once we properly align properties and columns.
    if (propertyID != common::INVALID_PROPERTY_ID) {
        tablesStatistics->getPropertyStatisticsForTable(transaction, tableID, propertyID)
            .setHasNull();
    }
}

} // namespace storage
} // namespace kuzu
