#pragma once

#include "common/data_chunk/data_chunk.h"
#include "processor/data_pos.h"

namespace kuzu {
namespace processor {

struct AggregateInputInfo {
    DataPos aggregateVectorPos;
    std::vector<data_chunk_pos_t> multiplicityChunksPos;

    AggregateInputInfo(
        const DataPos& vectorPos, std::vector<data_chunk_pos_t> multiplicityChunksPos)
        : aggregateVectorPos{vectorPos}, multiplicityChunksPos{std::move(multiplicityChunksPos)} {}
    AggregateInputInfo(const AggregateInputInfo& other)
        : AggregateInputInfo(other.aggregateVectorPos, other.multiplicityChunksPos) {}
    inline std::unique_ptr<AggregateInputInfo> copy() {
        return std::make_unique<AggregateInputInfo>(*this);
    }
};

struct AggregateInput {
    common::ValueVector* aggregateVector;
    std::vector<common::DataChunk*> multiplicityChunks;
};

} // namespace processor
} // namespace kuzu
