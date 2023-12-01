#include "processor/operator/filter.h"

using namespace kuzu::common;

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

void NodeLabelFiler::initLocalStateInternal(
    ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    nodeIDVector = resultSet->getValueVector(info->nodeVectorPos).get();
}

bool NodeLabelFiler::getNextTuplesInternal(ExecutionContext* context) {
    sel_t numSelectValue;
    do {
        restoreSelVector(nodeIDVector->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(nodeIDVector->state->selVector);
        numSelectValue = 0;
        auto buffer = nodeIDVector->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            buffer[numSelectValue] = pos;
            numSelectValue +=
                info->nodeLabelSet.contains(nodeIDVector->getValue<nodeID_t>(pos).tableID);
        }
        nodeIDVector->state->selVector->resetSelectorToValuePosBuffer();
    } while (numSelectValue == 0);
    nodeIDVector->state->selVector->selectedSize = numSelectValue;
    metrics->numOutputTuple.increase(nodeIDVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
