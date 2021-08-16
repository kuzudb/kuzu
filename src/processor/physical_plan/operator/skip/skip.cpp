#include "src/processor/include/physical_plan/operator/skip/skip.h"

namespace graphflow {
namespace processor {

Skip::Skip(uint64_t skipNumber, shared_ptr<atomic_uint64_t> counter, uint64_t dataChunkToSelectPos,
    vector<uint64_t> dataChunksToSkipPos, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SKIP, context, id},
      FilteringOperator(this->prevOperator->getResultSet()->dataChunks[dataChunkToSelectPos]),
      skipNumber{skipNumber}, counter{move(counter)}, dataChunkToSelectPos{dataChunkToSelectPos},
      dataChunksToSkipPos{move(dataChunksToSkipPos)} {
    resultSet = this->prevOperator->getResultSet();
}

void Skip::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

void Skip::getNextTuples() {
    metrics->executionTime.start();
    auto numTupleSkippedBefore = 0u;
    auto numTuplesAvailable = 1u;
    do {
        restoreDataChunkSelectorState();
        prevOperator->getNextTuples();
        for (auto& dataChunkToSkipPos : dataChunksToSkipPos) {
            numTuplesAvailable *=
                resultSet->dataChunks[dataChunkToSkipPos]->state->getNumSelectedValues();
        }
        // end of execution due to no more input
        if (numTuplesAvailable == 0) {
            return;
        }
        saveDataChunkSelectorState();
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
        auto& dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
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
}

} // namespace processor
} // namespace graphflow
