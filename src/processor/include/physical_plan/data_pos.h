#pragma once

namespace graphflow {
namespace processor {

struct DataPos {

public:
    DataPos(uint32_t dataChunkPos, uint32_t valueVectorPos)
        : dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos} {}

    DataPos(const DataPos& other) : DataPos(other.dataChunkPos, other.valueVectorPos) {}

public:
    uint32_t dataChunkPos;
    uint32_t valueVectorPos;
};

} // namespace processor
} // namespace graphflow
