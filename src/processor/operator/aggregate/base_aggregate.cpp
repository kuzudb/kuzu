#include "processor/operator/aggregate/base_aggregate.h"

#include "main/client_context.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

BaseAggregateSharedState::BaseAggregateSharedState(
    const std::vector<AggregateFunction>& aggregateFunctions, size_t numPartitions)
    : currentOffset{0}, aggregateFunctions{copyVector(aggregateFunctions)}, numThreads{0},
      // numPartitions - 1 since we want the bit width of the largest value that
      // could be used to index the partitions
      shiftForPartitioning{
          static_cast<uint8_t>(sizeof(common::hash_t) * 8 - std::bit_width(numPartitions - 1))} {}

void BaseAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& info : aggInfos) {
        auto aggregateInput = AggregateInput();
        if (info.aggVectorPos.dataChunkPos == INVALID_DATA_CHUNK_POS) {
            aggregateInput.aggregateVector = nullptr;
        } else {
            aggregateInput.aggregateVector = resultSet->getValueVector(info.aggVectorPos).get();
        }
        for (auto dataChunkPos : info.multiplicityChunksPos) {
            aggregateInput.multiplicityChunks.push_back(
                resultSet->getDataChunk(dataChunkPos).get());
        }
        aggInputs.push_back(std::move(aggregateInput));
    }
}

} // namespace processor
} // namespace kuzu
