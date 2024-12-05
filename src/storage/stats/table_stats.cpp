#include "storage/stats/table_stats.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {
namespace storage {

TableStats::TableStats(std::span<const common::LogicalType> dataTypes) : cardinality{0} {
    for (const auto& dataType : dataTypes) {
        columnStats.emplace_back(dataType);
    }
}

TableStats::TableStats(const TableStats& other) : cardinality{other.cardinality} {
    columnStats.reserve(other.columnStats.size());
    for (auto i = 0u; i < other.columnStats.size(); ++i) {
        columnStats.emplace_back(other.columnStats[i].copy());
    }
}

void TableStats::update(const std::vector<common::ValueVector*>& vectors, size_t numColumns) {
    size_t numColumnsToUpdate = std::min(numColumns, vectors.size());

    KU_ASSERT(numColumnsToUpdate == columnStats.size());
    for (auto i = 0u; i < numColumnsToUpdate; ++i) {
        columnStats[i].update(vectors[i]);
    }
    const auto numValues = vectors[0]->state->getSelVector().getSelSize();
    for (auto i = 1u; i < numColumnsToUpdate; ++i) {
        KU_ASSERT(vectors[i]->state->getSelVector().getSelSize() == numValues);
    }
    incrementCardinality(numValues);
}

void TableStats::serialize(common::Serializer& serializer) const {
    serializer.writeDebuggingInfo("cardinality");
    serializer.write(cardinality);
    serializer.writeDebuggingInfo("column_stats");
    serializer.serializeVector(columnStats);
}

TableStats TableStats::deserialize(common::Deserializer& deserializer) {
    std::string info;
    deserializer.validateDebuggingInfo(info, "cardinality");
    deserializer.deserializeValue<common::cardinality_t>(cardinality);
    deserializer.validateDebuggingInfo(info, "column_stats");
    deserializer.deserializeVector(columnStats);
    return *this;
}

} // namespace storage
} // namespace kuzu
