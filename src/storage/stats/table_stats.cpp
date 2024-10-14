#include "storage/stats/table_stats.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {
namespace storage {

TableStats::TableStats(const TableStats& other) : cardinality{other.cardinality} {
    columnStats.resize(other.columnStats.size());
    for (auto i = 0u; i < columnStats.size(); ++i) {
        columnStats[i] = other.columnStats[i].copy();
    }
}

void TableStats::update(const std::vector<common::ValueVector*>& vectors) {
    KU_ASSERT(vectors.size() == columnStats.size());
    for (auto i = 0u; i < vectors.size(); ++i) {
        columnStats[i].update(vectors[i]);
    }
    const auto numValues = vectors[0]->state->getSelVector().getSelSize();
    for (auto i = 1u; i < vectors.size(); ++i) {
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
