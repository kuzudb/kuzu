#include "src/processor/include/physical_plan/operator/limit/limit.h"

namespace graphflow {
namespace processor {

Limit::Limit(uint64_t limitNumber, shared_ptr<atomic_uint64_t> counter,
    uint64_t dataChunkToSelectPos, vector<uint64_t> dataChunksToLimitPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), LIMIT, context, id},
      limitNumber{limitNumber}, counter{move(counter)}, dataChunkToSelectPos{dataChunkToSelectPos},
      dataChunksToLimitPos(move(dataChunksToLimitPos)) {
    resultSet = this->prevOperator->getResultSet();
}

void Limit::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    auto numTupleAvailable = 1u;
    for (auto& dataChunkToLimitPos : dataChunksToLimitPos) {
        numTupleAvailable *=
            resultSet->dataChunks[dataChunkToLimitPos]->state->getNumSelectedValues();
    }
    // end of execution due to no more input
    if (numTupleAvailable == 0) {
        return;
    }
    auto numTupleProcessedBefore = counter->fetch_add(numTupleAvailable);
    if (numTupleProcessedBefore + numTupleAvailable > limitNumber) {
        int64_t numTupleToProcessInCurrentResultSet = limitNumber - numTupleProcessedBefore;
        // end of execution due to limit has reached
        if (numTupleToProcessInCurrentResultSet <= 0) {
            for (auto& dataChunk : resultSet->dataChunks) {
                dataChunk->state->currIdx = -1;
                dataChunk->state->selectedSize = 0;
            }
            return;
        } else {
            // If all dataChunks are flat, numTupleAvailable = 1 which means numTupleProcessedBefore
            // = limitNumber. So execution is terminated in above if statement.
            auto& dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
            assert(!dataChunkToSelect->state->isFlat());
            dataChunkToSelect->state->selectedSize = numTupleToProcessInCurrentResultSet;
            metrics->numOutputTuple.increase(dataChunkToSelect->state->selectedSize);
        }
    } else {
        metrics->numOutputTuple.increase(numTupleAvailable);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
