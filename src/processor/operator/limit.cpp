#include "processor/operator/limit.h"

namespace kuzu {
namespace processor {

bool Limit::getNextTuplesInternal(ExecutionContext* context) {
    // end of execution due to no more input
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto numTupleAvailable = resultSet->getNumTuples(dataChunksPosInScope);
    auto numTupleProcessedBefore = counter->fetch_add(numTupleAvailable);
    if (numTupleProcessedBefore + numTupleAvailable > limitNumber) {
        int64_t numTupleToProcessInCurrentResultSet = limitNumber - numTupleProcessedBefore;
        // end of execution due to limit has reached
        if (numTupleToProcessInCurrentResultSet <= 0) {
            return false;
        } else {
            // If all dataChunks are flat, numTupleAvailable = 1 which means numTupleProcessedBefore
            // = limitNumber. So execution is terminated in above if statement.
            auto& dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
            KU_ASSERT(!dataChunkToSelect->state->isFlat());
            dataChunkToSelect->state->selVector->selectedSize = numTupleToProcessInCurrentResultSet;
            metrics->numOutputTuple.increase(numTupleToProcessInCurrentResultSet);
        }
    } else {
        metrics->numOutputTuple.increase(numTupleAvailable);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
