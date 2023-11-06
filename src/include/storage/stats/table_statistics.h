#pragma once

#include "common/enums/table_type.h"
#include "storage/stats/property_statistics.h"

namespace kuzu {
namespace catalog {
class TableSchema;
}
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace storage {

class TableStatistics {
public:
    explicit TableStatistics(const catalog::TableSchema& schema);
    TableStatistics(common::TableType tableType, uint64_t numTuples, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics);
    TableStatistics(const TableStatistics& other);

    virtual ~TableStatistics() = default;

    inline bool isEmpty() const { return numTuples == 0; }
    inline uint64_t getNumTuples() const { return numTuples; }
    virtual inline void setNumTuples(uint64_t numTuples_) {
        KU_ASSERT(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

    inline PropertyStatistics& getPropertyStatistics(common::property_id_t propertyID) {
        KU_ASSERT(propertyStatistics.contains(propertyID));
        return *(propertyStatistics.at(propertyID));
    }
    inline void setPropertyStatistics(
        common::property_id_t propertyID, PropertyStatistics newStats) {
        propertyStatistics[propertyID] = std::make_unique<PropertyStatistics>(newStats);
    }

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<TableStatistics> deserialize(common::Deserializer& deserializer);
    virtual void serializeInternal(common::Serializer& serializer) = 0;

    virtual std::unique_ptr<TableStatistics> copy() = 0;

private:
    common::TableType tableType;
    uint64_t numTuples;
    common::table_id_t tableID;
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>
        propertyStatistics;
};

} // namespace storage
} // namespace kuzu
