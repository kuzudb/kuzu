#include "src/processor/include/physical_plan/operator/filter.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Filter::initResultSet() {
    resultSet = children[0]->initResultSet();
    expressionEvaluator->init(*resultSet, context.memoryManager);
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
        restoreDataChunkSelectorState(dataChunkToSelect);
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveDataChunkSelectorState(dataChunkToSelect);
        if (dataChunkToSelect->state->isFlat()) {
            hasAtLeastOneSelectedValue =
                expressionEvaluator->select(dataChunkToSelect->state->selectedPositions) > 0;
        } else {
            dataChunkToSelect->state->selectedSize = expressionEvaluator->select(
                dataChunkToSelect->state->selectedPositionsBuffer.get());
            if (dataChunkToSelect->state->isUnfiltered()) {
                dataChunkToSelect->state->resetSelectorToValuePosBuffer();
            }
            hasAtLeastOneSelectedValue = dataChunkToSelect->state->selectedSize > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(dataChunkToSelect->state->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
