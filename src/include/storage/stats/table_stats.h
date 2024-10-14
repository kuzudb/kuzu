#pragma once

#include "common/types/types.h"

namespace kuzu::common {
class LogicalType;
}
namespace kuzu {
namespace storage {

class TableStats {
public:
    explicit TableStats() : cardinality{0} {}
    EXPLICIT_COPY_DEFAULT_MOVE(TableStats);

    void incrementCardinality(common::cardinality_t increment) { cardinality += increment; }

    void merge(const TableStats& other) { cardinality += other.cardinality; }

    common::cardinality_t getCardinality() const { return cardinality; }

    void serialize(common::Serializer& serializer) const;
    TableStats deserialize(common::Deserializer& deserializer);

private:
    TableStats(const TableStats& other) : cardinality{other.cardinality} {}

private:
    // Note: cardinality is the estimated number of rows in the table. It is not always up-to-date.
    common::cardinality_t cardinality;
};

} // namespace storage
} // namespace kuzu
