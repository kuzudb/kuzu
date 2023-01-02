#pragma once

#include <cstdint>
#include <utility>

namespace kuzu {
namespace processor {

struct DataPos {
public:
    explicit DataPos(uint32_t dataChunkPos, uint32_t valueVectorPos)
        : dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos} {}
    explicit DataPos(std::pair<uint32_t, uint32_t> pos)
        : dataChunkPos{pos.first}, valueVectorPos{pos.second} {}

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
