#include "processor/operator/flatten.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Flatten::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    dataChunkToFlatten = resultSet->dataChunks[dataChunkToFlattenPos];
    currentSelVector->resetSelectorToValuePosBufferWithSize(1 /* size */);
}

bool Flatten::getNextTuplesInternal() {
    if (isCurrIdxInitialOrLast()) {
        dataChunkToFlatten->state->currIdx = -1;
        restoreSelVector(dataChunkToFlatten->state->selVector);
        if (!children[0]->getNextTuple()) {
            return false;
        }
        saveSelVector(dataChunkToFlatten->state->selVector);
    }
    dataChunkToFlatten->state->currIdx++;
    currentSelVector->selectedPositions[0] =
        prevSelVector->selectedPositions[dataChunkToFlatten->state->currIdx];
    metrics->numOutputTuple.incrementByOne();
    return true;
}

void Flatten::resetToCurrentSelVector(std::shared_ptr<SelectionVector>& selVector) {
    selVector = currentSelVector;
}

} // namespace processor
} // namespace kuzu
