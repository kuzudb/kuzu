#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/function_expression.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    BaseExpressionEvaluator::init(resultSet, memoryManager);
    execFunc = ((binder::ScalarFunctionExpression&)*expression).execFunc;
    if (expression->dataType.typeID == BOOL) {
        selectFunc = ((binder::ScalarFunctionExpression&)*expression).selectFunc;
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
    // Temporary code path for function whose return type is BOOL but select interface is not
    // implemented (e.g. list_contains). We should remove this if statement eventually.
    if (selectFunc == nullptr) {
        assert(resultVector->dataType.typeID == BOOL);
        execFunc(parameters, *resultVector);
        auto numSelectedValues = 0u;
        for (auto i = 0u; i < resultVector->state->selVector->selectedSize; ++i) {
            auto pos = resultVector->state->selVector->selectedPositions[i];
            auto selectedPosBuffer = selVector.getSelectedPositionsBuffer();
            selectedPosBuffer[numSelectedValues] = pos;
            numSelectedValues += resultVector->getValue<bool>(pos);
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }
    return selectFunc(parameters, selVector);
}

std::unique_ptr<BaseExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<BaseExpressionEvaluator>> clonedChildren;
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, std::move(clonedChildren));
}

void FunctionExpressionEvaluator::resolveResultVector(
    const ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    std::vector<BaseExpressionEvaluator*> inputEvaluators;
    for (auto& child : children) {
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
}

} // namespace evaluator
} // namespace kuzu
