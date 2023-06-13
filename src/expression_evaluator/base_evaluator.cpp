#include "expression_evaluator/base_evaluator.h"

namespace kuzu {
namespace evaluator {

void BaseExpressionEvaluator::init(
    const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) {
    for (auto& child : children) {
        child->init(resultSet, memoryManager);
    }
    resolveResultVector(resultSet, memoryManager);
}

void BaseExpressionEvaluator::resolveResultStateFromChildren(
    const std::vector<BaseExpressionEvaluator*>& inputEvaluators) {
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
    resultVector->setState(common::DataChunkState::getSingleValueDataChunkState());
}

} // namespace evaluator
} // namespace kuzu
