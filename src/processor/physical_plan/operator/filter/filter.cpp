#include "src/processor/include/physical_plan/operator/filter/filter.h"

namespace graphflow {
namespace processor {

void Filter::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    rootExpr->initResultSet(*this->resultSet, *context.memoryManager);
    dataChunkToSelect = this->resultSet->dataChunks[dataChunkToSelectPos];
}

void Filter::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

void Filter::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneSelectedValue;
    do {
        restoreDataChunkSelectorState(dataChunkToSelect);
        prevOperator->getNextTuples();
        if (dataChunkToSelect->state->selectedSize == 0) {
            break;
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
}

unique_ptr<PhysicalOperator> Filter::clone() {
    auto prevOperatorClone = prevOperator->clone();
    return make_unique<Filter>(
        rootExpr->clone(), dataChunkToSelectPos, move(prevOperatorClone), context, id);
}

} // namespace processor
} // namespace graphflow
