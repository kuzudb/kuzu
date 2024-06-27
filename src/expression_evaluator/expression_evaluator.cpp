#include "expression_evaluator/expression_evaluator.h"

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

} // namespace evaluator
} // namespace kuzu
