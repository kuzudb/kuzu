#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/function_expression.h"

namespace kuzu {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    execFunc = ((ScalarFunctionExpression&)*expression).execFunc;
    if (expression->dataType.typeID == BOOL) {
        selectFunc = ((ScalarFunctionExpression&)*expression).selectFunc;
    }
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
    }
}

void FunctionExpressionEvaluator::evaluate() {
    for (auto& child : children) {
        child->evaluate();
    }
    execFunc(parameters, *resultVector);
}

bool FunctionExpressionEvaluator::select(SelectionVector& selVector) {
    for (auto& child : children) {
        child->evaluate();
    }
    return selectFunc(parameters, selVector);
}

unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, move(clonedChildren));
}

void FunctionExpressionEvaluator::resolveResultVector(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = make_shared<ValueVector>(expression->dataType, memoryManager);
    vector<BaseExpressionEvaluator*> inputEvaluators;
    for (auto& child : children) {
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
}

} // namespace evaluator
} // namespace kuzu
