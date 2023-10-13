#pragma once

#include <map>

#include "storage/stats/table_statistics.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

class RelsStoreStats;
class RelTableStats : public TableStatistics {
    friend class RelsStoreStats;

public:
    RelTableStats(const catalog::TableSchema& tableSchema)
        : TableStatistics{tableSchema}, nextRelOffset{0} {}
    RelTableStats(uint64_t numRels, common::table_id_t tableID, common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, {}}, nextRelOffset{
                                                                             nextRelOffset} {}
    RelTableStats(uint64_t numRels, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStats,
        common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, std::move(propertyStats)},
          nextRelOffset{nextRelOffset} {}

    inline common::offset_t getNextRelOffset() const { return nextRelOffset; }

    void serializeInternal(common::Serializer& serializer) final;
    static std::unique_ptr<RelTableStats> deserialize(
        uint64_t numRels, common::table_id_t tableID, common::Deserializer& deserializer);

    inline std::unique_ptr<TableStatistics> copy() final {
        return std::make_unique<RelTableStats>(*this);
    }

private:
    common::offset_t nextRelOffset;
};

} // namespace storage
} // namespace kuzu
