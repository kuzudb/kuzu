#include "include/limit.h"

namespace kuzu {
namespace processor {

bool Limit::getNextTuples() {
    metrics->executionTime.start();
    // end of execution due to no more input
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    auto numTupleAvailable = resultSet->getNumTuples(dataChunksPosInScope);
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
            dataChunkToSelect->state->selVector->selectedSize = numTupleToProcessInCurrentResultSet;
            metrics->numOutputTuple.increase(numTupleToProcessInCurrentResultSet);
        }
    } else {
        metrics->numOutputTuple.increase(numTupleAvailable);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace kuzu
