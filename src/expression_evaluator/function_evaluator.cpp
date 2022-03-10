#include "include/function_evaluator.h"

namespace graphflow {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    getFunction();
    resultVector = make_shared<ValueVector>(memoryManager, expression->dataType);
    // set resultVector state to the state of its unFlat child if there is any
    assert(!children.empty());
    resultVector->state = children[0]->resultVector->state;
    for (auto& child : children) {
        if (!child->isResultVectorFlat()) {
            resultVector->state = child->resultVector->state;
            break;
        }
    }
    // extract function parameters
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
    }
}

void FunctionExpressionEvaluator::evaluate() {
    for (auto& child : children) {
        child->evaluate();
    }
    func(parameters, *resultVector);
}

unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, move(clonedChildren));
}

void FunctionExpressionEvaluator::getFunction() {
    switch (expression->expressionType) {
    case LIST_CREATION: {
        func = VectorListOperations::ListCreation;
    } break;
    default:
        throw invalid_argument(
            "Unsupported expression type: " + expressionTypeToString(expression->expressionType));
    }
}

} // namespace evaluator
} // namespace graphflow
