#include "include/function_evaluator.h"

#include "src/binder/expression/include/function_expression.h"
#include "src/function/list/include/vector_list_operations.h"

namespace graphflow {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    getExecFunction();
    if (expression->dataType == BOOL) {
        getSelectFunction();
    }
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

uint64_t FunctionExpressionEvaluator::select(sel_t* selectedPos) {
    for (auto& child : children) {
        child->evaluate();
    }
    return selectFunc(parameters, selectedPos);
}

unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, move(clonedChildren));
}

void FunctionExpressionEvaluator::getExecFunction() {
    switch (expression->expressionType) {
    case LIST_CREATION: {
        execFunc = VectorListOperations::ListCreation;
    } break;
    case AND:
    case OR:
    case XOR:
    case NOT:
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_EQUALS:
    case LESS_THAN:
    case LESS_THAN_EQUALS:
    case STRING_CONCAT:
    case STARTS_WITH:
    case CONTAINS:
    case IS_NULL:
    case IS_NOT_NULL: {
        execFunc = ((ScalarFunctionExpression&)*expression).execFunc;
    } break;
    default:
        throw invalid_argument(
            "Unsupported expression type: " + expressionTypeToString(expression->expressionType));
    }
}

void FunctionExpressionEvaluator::getSelectFunction() {
    switch (expression->expressionType) {
    case AND:
    case OR:
    case XOR:
    case NOT:
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_EQUALS:
    case LESS_THAN:
    case LESS_THAN_EQUALS:
    case STARTS_WITH:
    case CONTAINS:
    case IS_NULL:
    case IS_NOT_NULL: {
        selectFunc = ((ScalarFunctionExpression&)*expression).selectFunc;
    } break;
    default:
        throw invalid_argument(
            "Unsupported expression type: " + expressionTypeToString(expression->expressionType));
    }
}

} // namespace evaluator
} // namespace graphflow
