#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/function_expression.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace evaluator {

void FunctionExpressionEvaluator::init(const ResultSet& resultSet,
    main::ClientContext* clientContext) {
    ExpressionEvaluator::init(resultSet, clientContext);
    auto& functionExpr = expression->constCast<binder::ScalarFunctionExpression>();
    execFunc = functionExpr.execFunc;
    if (expression->dataType.getLogicalTypeID() == LogicalTypeID::BOOL) {
        selectFunc = ((binder::ScalarFunctionExpression&)*expression).selectFunc;
    }
    bindData = expression->constPtrCast<binder::ScalarFunctionExpression>()->getBindData()->copy();
}

void FunctionExpressionEvaluator::evaluate() {
    auto cnt = localState.count;
    auto ctx = localState.clientContext;
    for (auto& child : children) {
        child->evaluate();
    }
    if (execFunc != nullptr) {
        bindData->clientContext = ctx;
        bindData->count = cnt;
        execFunc(parameters, *resultVector, bindData.get());
    }
}

bool FunctionExpressionEvaluator::select(SelectionVector& selVector) {
    for (auto& child : children) {
        child->evaluate();
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
            numSelectedValues += resultVector->isNull(pos) ? 0 : resultVector->getValue<bool>(pos);
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }
    return selectFunc(parameters, selVector);
}

void FunctionExpressionEvaluator::resolveResultVector(const ResultSet& /*resultSet*/,
    MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->dataType.copy(), memoryManager);
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
