#include "processor/operator/filter.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Filter::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->clientContext->getMemoryManager());
    KU_ASSERT(dataChunkToSelectPos != INVALID_DATA_CHUNK_POS);
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
        hasAtLeastOneSelectedValue = expressionEvaluator->select(
            *dataChunkToSelect->state->selVector, context->clientContext);
        if (!dataChunkToSelect->state->isFlat() &&
            dataChunkToSelect->state->selVector->isUnfiltered()) {
            dataChunkToSelect->state->selVector->setToFiltered();
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->numOutputTuple.increase(dataChunkToSelect->state->selVector->selectedSize);
    return true;
}

void NodeLabelFiler::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
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
        auto buffer = nodeIDVector->state->selVector->getMultableBuffer();
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            buffer[numSelectValue] = pos;
            numSelectValue +=
                info->nodeLabelSet.contains(nodeIDVector->getValue<nodeID_t>(pos).tableID);
        }
        nodeIDVector->state->selVector->setToFiltered();
    } while (numSelectValue == 0);
    nodeIDVector->state->selVector->selectedSize = numSelectValue;
    metrics->numOutputTuple.increase(nodeIDVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
