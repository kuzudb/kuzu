#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/function_expression.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    ExpressionEvaluator::init(resultSet, memoryManager);
    execFunc = ((binder::ScalarFunctionExpression&)*expression).execFunc;
    if (expression->dataType.getLogicalTypeID() == LogicalTypeID::BOOL) {
        selectFunc = ((binder::ScalarFunctionExpression&)*expression).selectFunc;
    }
}

void FunctionExpressionEvaluator::evaluate(ClientContext* clientContext) {
    for (auto& child : children) {
        child->evaluate(clientContext);
    }
    auto expr = expression->constPtrCast<binder::ScalarFunctionExpression>();
    if (execFunc != nullptr) {
        auto bindData = expr->getBindData();
        bindData->clientContext = clientContext;
        execFunc(parameters, *resultVector, bindData);
    }
}

bool FunctionExpressionEvaluator::select(SelectionVector& selVector,
    ClientContext* /*ClientContext*/) {
    for (auto& child : children) {
        child->evaluate(nullptr);
    }
    // Temporary code path for function whose return type is BOOL but select interface is not
    // implemented (e.g. list_contains). We should remove this if statement eventually.
    if (selectFunc == nullptr) {
        KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
        execFunc(parameters, *resultVector, nullptr);
        auto numSelectedValues = 0u;
        for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); ++i) {
            auto pos = resultVector->state->getSelVector()[i];
            auto selectedPosBuffer = selVector.getMultableBuffer();
            selectedPosBuffer[numSelectedValues] = pos;
            numSelectedValues += resultVector->getValue<bool>(pos);
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }
    return selectFunc(parameters, selVector);
}

std::unique_ptr<ExpressionEvaluator> FunctionExpressionEvaluator::clone() {
    std::vector<std::unique_ptr<ExpressionEvaluator>> clonedChildren;
    clonedChildren.reserve(children.size());
    for (auto& child : children) {
        clonedChildren.push_back(child->clone());
    }
    return make_unique<FunctionExpressionEvaluator>(expression, std::move(clonedChildren));
}

void FunctionExpressionEvaluator::resolveResultVector(const ResultSet& /*resultSet*/,
    MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType, memoryManager);
    std::vector<ExpressionEvaluator*> inputEvaluators;
    inputEvaluators.reserve(children.size());
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
    auto& functionExpression = (binder::ScalarFunctionExpression&)*expression;
    if (functionExpression.compileFunc != nullptr) {
        functionExpression.compileFunc(functionExpression.getBindData(), parameters, resultVector);
    }
}

} // namespace evaluator
} // namespace kuzu
