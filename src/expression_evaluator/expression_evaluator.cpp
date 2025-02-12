#include "expression_evaluator/expression_evaluator.h"

#include "common/exception/runtime.h"

using namespace kuzu::common;

namespace kuzu {
namespace evaluator {

void ExpressionEvaluator::init(const processor::ResultSet& resultSet,
    main::ClientContext* clientContext) {
    localState.clientContext = clientContext;
    for (auto& child : children) {
        child->init(resultSet, clientContext);
    }
    resolveResultVector(resultSet, clientContext->getMemoryManager());
}

void ExpressionEvaluator::resolveResultStateFromChildren(
    const std::vector<ExpressionEvaluator*>& inputEvaluators) {
    if (resultVector->state != nullptr) {
        return;
    }
    for (auto& input : inputEvaluators) {
        if (!input->isResultFlat()) {
            isResultFlat_ = false;
            resultVector->setState(input->resultVector->state);
            return;
        }
    }
    // All children are flat.
    isResultFlat_ = true;
    // We need to leave capacity for multiple evaluations
    resultVector->setState(std::make_shared<DataChunkState>());
    resultVector->state->initOriginalAndSelectedSize(1);
    resultVector->state->setToFlat();
}

void ExpressionEvaluator::evaluate(common::sel_t) {
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat("Cannot evaluate expression {} with count. This should "
                                        "never happen.",
        expression->toString()));
    // LCOV_EXCL_STOP
}

bool ExpressionEvaluator::select(common::SelectionVector& selVector,
    bool shouldSetSelVectorToFiltered) {
    bool ret = selectInternal(selVector);
    if (shouldSetSelVectorToFiltered && selVector.isUnfiltered()) {
        selVector.setToFiltered();
    }
    return ret;
}

} // namespace evaluator
} // namespace kuzu
