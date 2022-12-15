#include "expression_evaluator/base_evaluator.h"

namespace kuzu {
namespace evaluator {

void BaseExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    for (auto& child : children) {
        child->init(resultSet, memoryManager);
    }
    resolveResultVector(resultSet, memoryManager);
}

void BaseExpressionEvaluator::resolveResultStateFromChildren(
    const vector<BaseExpressionEvaluator*>& inputEvaluators) {
    for (auto& input : inputEvaluators) {
        if (!input->isResultFlat()) {
            isResultFlat_ = false;
            resultVector->state = input->resultVector->state;
            return;
        }
    }
    // All children are flat.
    isResultFlat_ = true;
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

} // namespace evaluator
} // namespace kuzu
