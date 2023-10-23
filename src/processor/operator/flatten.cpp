#include "processor/operator/flatten.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Flatten::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    dataChunkState = resultSet->dataChunks[dataChunkToFlattenPos]->state.get();
    currentSelVector->resetSelectorToValuePosBufferWithSize(1 /* size */);
    localState = std::make_unique<FlattenLocalState>();
}

bool Flatten::getNextTuplesInternal(ExecutionContext* context) {
    if (localState->currentIdx == localState->sizeToFlatten) {
        dataChunkState->setToUnflat(); // TODO(Xiyang): this should be part of restore/save
        restoreSelVector(dataChunkState->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        localState->currentIdx = 0;
        localState->sizeToFlatten = dataChunkState->selVector->selectedSize;
        saveSelVector(dataChunkState->selVector);
        dataChunkState->setToFlat();
    }
    currentSelVector->selectedPositions[0] =
        prevSelVector->selectedPositions[localState->currentIdx++];
    metrics->numOutputTuple.incrementByOne();
    return true;
}

void Flatten::resetToCurrentSelVector(std::shared_ptr<SelectionVector>& selVector) {
    selVector = currentSelVector;
}

} // namespace processor
} // namespace kuzu
