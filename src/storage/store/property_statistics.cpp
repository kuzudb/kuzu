#include "storage/store/property_statistics.h"

#include "common/ser_deser.h"
#include "storage/store/table_statistics.h"

namespace kuzu {
namespace storage {

void PropertyStatistics::serialize(common::FileInfo* fileInfo, uint64_t& offset) {
    common::SerDeser::serializeValue(mayHaveNullValue, fileInfo, offset);
}

RWPropertyStats::RWPropertyStats(TablesStatistics* tablesStatistics, common::table_id_t tableID,
    common::property_id_t propertyID)
    : tablesStatistics{tablesStatistics}, tableID{tableID}, propertyID{propertyID} {}

std::unique_ptr<PropertyStatistics> PropertyStatistics::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    bool hasNull;
    common::SerDeser::deserializeValue<bool>(hasNull, fileInfo, offset);
    return std::make_unique<PropertyStatistics>(hasNull);
}

// In a read-only context, readStatistics should always be non-null
// Read/write statistics cannot be cached since functions like checkpointInMemoryIfNecessary may
// overwrite them and invalidate the reference
bool RWPropertyStats::mayHaveNull() {
    auto readStatistics = tablesStatistics->getReadPropertyStatisticsForTable(tableID, propertyID);
    if (readStatistics) {
        return readStatistics->mayHaveNull();
    }

    auto writeStatistics =
        tablesStatistics->getWritePropertyStatisticsForTable(tableID, propertyID);
    return writeStatistics.mayHaveNull();
}

void RWPropertyStats::setHasNull() {
    tablesStatistics->getWritePropertyStatisticsForTable(tableID, propertyID).setHasNull();
}

} // namespace storage
} // namespace kuzu
