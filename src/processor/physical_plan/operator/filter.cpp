#include "src/processor/include/physical_plan/operator/filter.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Filter::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    expressionEvaluator->init(*resultSet, context->memoryManager);
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
    return resultSet;
}

void Filter::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
    FilteringOperator::reInitToRerunSubPlan();
}

bool Filter::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneSelectedValue;
    do {
        restoreSelVector(dataChunkToSelect->state->selVector.get());
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
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
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(dataChunkToSelect->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
