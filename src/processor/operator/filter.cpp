#include "processor/operator/filter.h"

#include "common/exception/runtime.h"
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string FilterPrintInfo::toString() const {
    return expression->toString();
}

void Filter::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->clientContext);
    // LCOV_EXCL_START
    if (dataChunkToSelectPos == INVALID_DATA_CHUNK_POS) {
        throw RuntimeException(stringFormat("Trying to evaluate constant expression {} at runtime. "
                                            "This should be done at compile time.",
            expressionEvaluator->getExpression()->toString()));
    }
    // LCOV_EXCL_STOP
    dataChunkToSelect = resultSet->dataChunks[dataChunkToSelectPos];
}

bool Filter::getNextTuplesInternal(ExecutionContext* context) {
    bool hasAtLeastOneSelectedValue;
    do {
        restoreSelVector(*dataChunkToSelect->state);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(*dataChunkToSelect->state);
        hasAtLeastOneSelectedValue =
            expressionEvaluator->select(dataChunkToSelect->state->getSelVectorUnsafe());
        if (!dataChunkToSelect->state->isFlat() &&
            dataChunkToSelect->state->getSelVector().isUnfiltered()) {
            dataChunkToSelect->state->getSelVectorUnsafe().setToFiltered();
        }
    } while (!hasAtLeastOneSelectedValue);
    metrics->numOutputTuple.increase(dataChunkToSelect->state->getSelVector().getSelSize());
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
