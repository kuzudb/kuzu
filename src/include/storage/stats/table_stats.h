#pragma once

#include "common/types/types.h"
#include "storage/stats/column_stats.h"

namespace kuzu::common {
class LogicalType;
}
namespace kuzu {
namespace storage {

class TableStats {
public:
    explicit TableStats(std::span<const common::LogicalType> dataTypes);

    EXPLICIT_COPY_DEFAULT_MOVE(TableStats);

    void incrementCardinality(common::cardinality_t increment) { cardinality += increment; }

    void merge(const TableStats& other) {
        cardinality += other.cardinality;
        KU_ASSERT(columnStats.size() == other.columnStats.size());
        for (auto i = 0u; i < columnStats.size(); ++i) {
            columnStats[i].merge(other.columnStats[i]);
        }
    }

    common::cardinality_t getTableCard() const { return cardinality; }

    common::cardinality_t getNumDistinctValues(common::column_id_t columnID) const {
        KU_ASSERT(columnID < columnStats.size());
        return columnStats[columnID].getNumDistinctValues();
    }

    void update(const std::vector<common::ValueVector*>& vectors,
        size_t numColumns = std::numeric_limits<size_t>::max());

    ColumnStats& addNewColumn(const common::LogicalType& dataType) {
        columnStats.emplace_back(dataType);
        return columnStats.back();
    }

    void serialize(common::Serializer& serializer) const;
    TableStats deserialize(common::Deserializer& deserializer);

private:
    TableStats(const TableStats& other);

private:
    // Note: cardinality is the estimated number of rows in the table. It is not always up-to-date.
    common::cardinality_t cardinality;
    std::vector<ColumnStats> columnStats;
};

} // namespace storage
} // namespace kuzu
