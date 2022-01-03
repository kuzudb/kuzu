#include "src/processor/include/physical_plan/operator/filter.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Filter::initResultSet() {
    resultSet = children[0]->initResultSet();
    rootExpr->initResultSet(*resultSet, *context.memoryManager);
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
                rootExpr->select(dataChunkToSelect->state->selectedPositions) > 0;
        } else {
            dataChunkToSelect->state->selectedSize =
                rootExpr->select(dataChunkToSelect->state->selectedPositionsBuffer.get());
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
