#include "src/processor/include/physical_plan/operator/limit.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Limit::initResultSet() {
    resultSet = prevOperator->initResultSet();
    return resultSet;
}

bool Limit::getNextTuples() {
    metrics->executionTime.start();
    // end of execution due to no more input
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    auto numTupleAvailable = 1u;
    for (auto& dataChunkToLimitPos : dataChunksToLimitPos) {
        numTupleAvailable *=
            resultSet->dataChunks[dataChunkToLimitPos]->state->getNumSelectedValues();
    }
    auto numTupleProcessedBefore = counter->fetch_add(numTupleAvailable);
    if (numTupleProcessedBefore + numTupleAvailable > limitNumber) {
        int64_t numTupleToProcessInCurrentResultSet = limitNumber - numTupleProcessedBefore;
        // end of execution due to limit has reached
        if (numTupleToProcessInCurrentResultSet <= 0) {
            metrics->executionTime.stop();
            return false;
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
    return true;
}

} // namespace processor
} // namespace graphflow
