#include "processor/operator/filter.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string FilterPrintInfo::toString() const {
    return expression->toString();
}

void Filter::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->clientContext);
    if (dataChunkToSelectPos == INVALID_DATA_CHUNK_POS) {
        // Filter a constant expression. Ideally we should fold all such expression at compile time.
        // But there are many edge cases, so we keep this code path for robustness.
        state = DataChunkState::getSingleValueDataChunkState();
    } else {
        state = resultSet->dataChunks[dataChunkToSelectPos]->state;
    }
}

bool Filter::getNextTuplesInternal(ExecutionContext* context) {
    bool hasAtLeastOneSelectedValue;
    do {
        restoreSelVector(*state);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(*state);
        hasAtLeastOneSelectedValue = expressionEvaluator->select(state->getSelVectorUnsafe());
        if (!state->isFlat() && state->getSelVector().isUnfiltered()) {
            state->getSelVectorUnsafe().setToFiltered();
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->numOutputTuple.increase(state->getSelVector().getSelSize());
    return true;
}

void NodeLabelFiler::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
    nodeIDVector = resultSet->getValueVector(info->nodeVectorPos).get();
}

bool NodeLabelFiler::getNextTuplesInternal(ExecutionContext* context) {
    sel_t numSelectValue;
    do {
        restoreSelVector(*nodeIDVector->state);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(*nodeIDVector->state);
        numSelectValue = 0;
        auto buffer = nodeIDVector->state->getSelVectorUnsafe().getMultableBuffer();
        for (auto i = 0u; i < nodeIDVector->state->getSelVector().getSelSize(); ++i) {
            auto pos = nodeIDVector->state->getSelVector()[i];
            buffer[numSelectValue] = pos;
            numSelectValue +=
                info->nodeLabelSet.contains(nodeIDVector->getValue<nodeID_t>(pos).tableID);
        }
        nodeIDVector->state->getSelVectorUnsafe().setToFiltered();
    } while (numSelectValue == 0);
    nodeIDVector->state->getSelVectorUnsafe().setSelSize(numSelectValue);
    metrics->numOutputTuple.increase(nodeIDVector->state->getSelVector().getSelSize());
    return true;
}

} // namespace processor
} // namespace kuzu
