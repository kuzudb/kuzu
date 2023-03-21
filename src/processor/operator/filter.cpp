#include "processor/operator/filter.h"

namespace kuzu {
namespace processor {

void Filter::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->memoryManager);
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
}

bool Filter::getNextTuplesInternal(ExecutionContext* context) {
    bool hasAtLeastOneSelectedValue;
    do {
        restoreSelVector(dataChunkToSelect->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(dataChunkToSelect->state->selVector);
        hasAtLeastOneSelectedValue =
            expressionEvaluator->select(*dataChunkToSelect->state->selVector);
        if (!dataChunkToSelect->state->isFlat() &&
            dataChunkToSelect->state->selVector->isUnfiltered()) {
            dataChunkToSelect->state->selVector->resetSelectorToValuePosBuffer();
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->numOutputTuple.increase(dataChunkToSelect->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
