#include "processor/operator/aggregate/base_aggregate.h"

#include "main/client_context.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

size_t getNumPartitionsForParallelism(main::ClientContext* context) {
    return context->getMaxNumThreadForExec();
}

BaseAggregateSharedState::BaseAggregateSharedState(
    const std::vector<AggregateFunction>& aggregateFunctions, size_t numPartitions)
    : currentOffset{0}, aggregateFunctions{copyVector(aggregateFunctions)}, numThreads{0},
      // numPartitions - 1 since we want the bit width of the largest value that
      // could be used to index the partitions
      shiftForPartitioning{
          static_cast<uint8_t>(sizeof(common::hash_t) * 8 - std::bit_width(numPartitions - 1))},
      readyForFinalization{false} {}

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

BaseAggregateSharedState::HashTableQueue::HashTableQueue(storage::MemoryManager* memoryManager,
    FactorizedTableSchema tableSchema) {
    headBlock = new TupleBlock(memoryManager, std::move(tableSchema));
    numTuplesPerBlock = headBlock.load()->table.getNumTuplesPerBlock();
}

BaseAggregateSharedState::HashTableQueue::~HashTableQueue() {
    delete headBlock.load();
    TupleBlock* block = nullptr;
    while (queuedTuples.pop(block)) {
        delete block;
    }
}

} // namespace processor
} // namespace kuzu
