#include "processor/operator/filter.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> Filter::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    expressionEvaluator->init(*resultSet, context->memoryManager);
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
    return resultSet;
}

bool Filter::getNextTuplesInternal() {
    bool hasAtLeastOneSelectedValue;
    do {
        restoreSelVector(dataChunkToSelect->state->selVector.get());
        if (!children[0]->getNextTuple()) {
            return false;
        }
        saveSelVector(dataChunkToSelect->state->selVector.get());
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
