#include "expression_evaluator/expression_evaluator.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace evaluator {

void ExpressionEvaluator::init(const processor::ResultSet& resultSet,
    main::ClientContext* clientContext) {
    localState.clientContext = clientContext;
    localState.count = 1;
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

void ExpressionEvaluator::updateCount(uint64_t newCount) {
    if (binder::ExpressionUtil::isCastExpr(*expression)) {
        KU_ASSERT(children.size() == 1);
        children[0]->updateCount(newCount);
    }
    localState.count = newCount;
}

} // namespace evaluator
} // namespace kuzu
