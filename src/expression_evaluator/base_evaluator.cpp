#include "expression_evaluator/base_evaluator.h"

namespace kuzu {
namespace evaluator {

BaseExpressionEvaluator::BaseExpressionEvaluator(unique_ptr<BaseExpressionEvaluator> child) {
    children.push_back(move(child));
}

BaseExpressionEvaluator::BaseExpressionEvaluator(
    unique_ptr<BaseExpressionEvaluator> left, unique_ptr<BaseExpressionEvaluator> right) {
    children.push_back(move(left));
    children.push_back(move(right));
}

void BaseExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    for (auto& child : children) {
        child->init(resultSet, memoryManager);
    }
}

} // namespace evaluator
} // namespace kuzu
