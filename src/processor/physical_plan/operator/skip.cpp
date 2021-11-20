#include "src/processor/include/physical_plan/operator/skip.h"

namespace graphflow {
namespace processor {

void Skip::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

bool Skip::getNextTuples() {
    metrics->executionTime.start();
    auto& dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
    auto numTupleSkippedBefore = 0u;
    auto numTuplesAvailable = 1u;
    do {
        restoreDataChunkSelectorState(dataChunkToSelect);
        // end of execution due to no more input
        if (!prevOperator->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveDataChunkSelectorState(dataChunkToSelect);
        for (auto& dataChunkToSkipPos : dataChunksToSkipPos) {
            numTuplesAvailable *=
                resultSet->dataChunks[dataChunkToSkipPos]->state->getNumSelectedValues();
        }
        numTupleSkippedBefore = counter->fetch_add(numTuplesAvailable);
    } while (numTupleSkippedBefore + numTuplesAvailable <= skipNumber);
    int64_t numTupleToSkipInCurrentResultSet = skipNumber - numTupleSkippedBefore;
    if (numTupleToSkipInCurrentResultSet <= 0) {
        // Other thread has finished skipping. Process everything in current result set.
        metrics->numOutputTuple.increase(numTuplesAvailable);
    } else {
        // If all dataChunks are flat, numTupleAvailable = 1 which means numTupleSkippedBefore =
        // skipNumber. So execution is handled in above if statement.
        assert(!dataChunkToSelect->state->isFlat());
        auto selectedPosBuffer = dataChunkToSelect->state->selectedPositionsBuffer.get();
        if (dataChunkToSelect->state->isUnfiltered()) {
            for (uint64_t i = numTupleToSkipInCurrentResultSet;
                 i < dataChunkToSelect->state->selectedSize; ++i) {
                selectedPosBuffer[i - numTupleToSkipInCurrentResultSet] = i;
            }
            dataChunkToSelect->state->resetSelectorToValuePosBuffer();
        } else {
            for (uint64_t i = numTupleToSkipInCurrentResultSet;
                 i < dataChunkToSelect->state->selectedSize; ++i) {
                selectedPosBuffer[i - numTupleToSkipInCurrentResultSet] = selectedPosBuffer[i];
            }
        }
        dataChunkToSelect->state->selectedSize -= numTupleToSkipInCurrentResultSet;
        metrics->numOutputTuple.increase(dataChunkToSelect->state->selectedSize);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
