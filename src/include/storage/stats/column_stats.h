#pragma once

#include "storage/stats/hyperloglog.h"
#include <common/serializer/deserializer.h>
#include <common/serializer/serializer.h>

namespace kuzu {
namespace storage {

class ColumnStats {
public:
    ColumnStats() : hashes{nullptr} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ColumnStats);

    common::cardinality_t getNumDistinctValues() const { return hll.count(); }

    void update(const common::ValueVector* vector);

    void merge(const ColumnStats& other) { hll.merge(other.hll); }

    void serialize(common::Serializer& serializer) const {
        serializer.writeDebuggingInfo("hll");
        hll.serialize(serializer);
    }
    static ColumnStats deserialize(common::Deserializer& deserializer) {
        ColumnStats columnStats;
        std::string info;
        deserializer.validateDebuggingInfo(info, "hll");
        columnStats.hll = HyperLogLog::deserialize(deserializer);
        return columnStats;
    }

private:
    ColumnStats(const ColumnStats& other) : hll{other.hll}, hashes{nullptr} {}

private:
    HyperLogLog hll;
    // Preallocated vector for hash values.
    std::unique_ptr<common::ValueVector> hashes;
};

} // namespace storage
} // namespace kuzu
