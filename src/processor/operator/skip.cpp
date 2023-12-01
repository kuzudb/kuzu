#include "processor/operator/skip.h"

namespace kuzu {
namespace processor {

void Skip::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
}

bool Skip::getNextTuplesInternal(ExecutionContext* context) {
    auto numTupleSkippedBefore = 0u;
    auto numTuplesAvailable = 1u;
    do {
        restoreSelVector(dataChunkToSelect->state->selVector);
        // end of execution due to no more input
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(dataChunkToSelect->state->selVector);
        numTuplesAvailable = resultSet->getNumTuples(dataChunksPosInScope);
        numTupleSkippedBefore = counter->fetch_add(numTuplesAvailable);
    } while (numTupleSkippedBefore + numTuplesAvailable <= skipNumber);
    auto numTupleToSkipInCurrentResultSet = (int64_t)(skipNumber - numTupleSkippedBefore);
    if (numTupleToSkipInCurrentResultSet <= 0) {
        // Other thread has finished skipping. Process everything in current result set.
        metrics->numOutputTuple.increase(numTuplesAvailable);
    } else {
        // If all dataChunks are flat, numTupleAvailable = 1 which means numTupleSkippedBefore =
        // skipNumber. So execution is handled in above if statement.
        KU_ASSERT(!dataChunkToSelect->state->isFlat());
        auto selectedPosBuffer = dataChunkToSelect->state->selVector->getSelectedPositionsBuffer();
        if (dataChunkToSelect->state->selVector->isUnfiltered()) {
            for (uint64_t i = numTupleToSkipInCurrentResultSet;
                 i < dataChunkToSelect->state->selVector->selectedSize; ++i) {
                selectedPosBuffer[i - numTupleToSkipInCurrentResultSet] = i;
            }
            dataChunkToSelect->state->selVector->resetSelectorToValuePosBuffer();
        } else {
            for (uint64_t i = numTupleToSkipInCurrentResultSet;
                 i < dataChunkToSelect->state->selVector->selectedSize; ++i) {
                selectedPosBuffer[i - numTupleToSkipInCurrentResultSet] = selectedPosBuffer[i];
            }
        }
        dataChunkToSelect->state->selVector->selectedSize =
            dataChunkToSelect->state->selVector->selectedSize - numTupleToSkipInCurrentResultSet;
        metrics->numOutputTuple.increase(dataChunkToSelect->state->selVector->selectedSize);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
