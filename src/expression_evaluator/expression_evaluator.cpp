#include "expression_evaluator/expression_evaluator.h"

using namespace kuzu::common;

namespace kuzu {
namespace evaluator {

void ExpressionEvaluator::init(const processor::ResultSet& resultSet,
    storage::MemoryManager* memoryManager) {
    for (auto& child : children) {
        child->init(resultSet, memoryManager);
    }
    resolveResultVector(resultSet, memoryManager);
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
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
}

std::vector<std::unique_ptr<ExpressionEvaluator>> ExpressionEvaluator::copy(
    const std::vector<std::unique_ptr<ExpressionEvaluator>>& evaluators) {
    std::vector<std::unique_ptr<ExpressionEvaluator>> result;
    for (auto& e : evaluators) {
        result.push_back(e->clone());
    }
    return result;
}

} // namespace evaluator
} // namespace kuzu
