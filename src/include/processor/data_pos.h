#pragma once

#include <cstdint>

namespace kuzu {
namespace processor {

struct DataPos {

public:
    DataPos(uint32_t dataChunkPos, uint32_t valueVectorPos)
        : dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos} {}

    DataPos(const DataPos& other) : DataPos(other.dataChunkPos, other.valueVectorPos) {}

    inline bool operator==(const DataPos& rhs) const {
        return (dataChunkPos == rhs.dataChunkPos) && (valueVectorPos == rhs.valueVectorPos);
    }

public:
    uint32_t dataChunkPos;
    uint32_t valueVectorPos;
};

} // namespace processor
} // namespace kuzu
