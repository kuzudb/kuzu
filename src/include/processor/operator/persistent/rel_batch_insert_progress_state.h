#pragma once

namespace kuzu {
namespace processor {
struct RelBatchInsertProgressSharedState {
    uint64_t partitionsDone;
    uint64_t partitionsTotal;

    RelBatchInsertProgressSharedState() : partitionsDone{0}, partitionsTotal{0} {};
};
} // namespace processor
} // namespace kuzu
