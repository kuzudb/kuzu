#include "processor/operator/flatten.h"

namespace kuzu {
namespace processor {

void Flatten::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    dataChunkToFlatten = resultSet->dataChunks[dataChunkToFlattenPos];
    unFlattenedSelVector = dataChunkToFlatten->state->selVector;
    flattenedSelVector = make_shared<SelectionVector>(1 /* capacity */);
    flattenedSelVector->resetSelectorToValuePosBufferWithSize(1 /* size */);
    dataChunkToFlatten->state->selVector = flattenedSelVector;
}

bool Flatten::getNextTuplesInternal() {
    if (isCurrIdxInitialOrLast()) {
        dataChunkToFlatten->state->currIdx = -1;
        dataChunkToFlatten->state->selVector = unFlattenedSelVector;
        if (!children[0]->getNextTuple()) {
            return false;
        }
        dataChunkToFlatten->state->selVector = flattenedSelVector;
    }
    dataChunkToFlatten->state->currIdx++;
    flattenedSelVector->selectedPositions[0] =
        unFlattenedSelVector->selectedPositions[dataChunkToFlatten->state->currIdx];
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
