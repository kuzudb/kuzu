#include "expression_evaluator/function_evaluator.h"

#include "binder/expression/scalar_function_expression.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::binder;

namespace kuzu {
namespace evaluator {

FunctionExpressionEvaluator::FunctionExpressionEvaluator(std::shared_ptr<Expression> expression,
    std::vector<std::unique_ptr<ExpressionEvaluator>> children)
    : ExpressionEvaluator{type_, std::move(expression), std::move(children)} {
    auto& functionExpr = this->expression->constCast<ScalarFunctionExpression>();
    function = functionExpr.getFunction().copy();
    bindData = functionExpr.getBindData()->copy();
}

void FunctionExpressionEvaluator::evaluate() {
    auto cnt = localState.count;
    auto ctx = localState.clientContext;
    for (auto& child : children) {
        child->evaluate();
    }
    if (function->execFunc != nullptr) {
        bindData->clientContext = ctx;
        bindData->count = cnt;
        function->execFunc(parameters, *resultVector, bindData.get());
    }
}

bool FunctionExpressionEvaluator::select(SelectionVector& selVector) {
    for (auto& child : children) {
        child->evaluate();
    }
    // Temporary code path for function whose return type is BOOL but select interface is not
    // implemented (e.g. list_contains). We should remove this if statement eventually.
    if (function->selectFunc == nullptr) {
        KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
        function->execFunc(parameters, *resultVector, nullptr);
        auto& resultSelVector = resultVector->state->getSelVector();
        if (resultSelVector.getSelSize() > 1) {
            auto numSelectedValues = 0u;
            for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); ++i) {
                auto pos = resultVector->state->getSelVector()[i];
                auto selectedPosBuffer = selVector.getMutableBuffer();
                selectedPosBuffer[numSelectedValues] = pos;
                numSelectedValues +=
                    resultVector->isNull(pos) ? 0 : resultVector->getValue<bool>(pos);
            }
            selVector.setSelSize(numSelectedValues);
            return numSelectedValues > 0;
        } else {
            // If result state is flat (i.e. all children are flat), we shouldn't try to update
            // selectedPos because we don't know which one is leading, i.e. the one being selected
            // by filter.
            // So we forget about selectedPos and directly return true/false. This doesn't change
            // the correctness, because when all children are flat the check is done on tuple.
            auto pos = resultVector->state->getSelVector()[0];
            return resultVector->isNull(pos) ? 0 : resultVector->getValue<bool>(pos);
            ;
        }
    }
    return function->selectFunc(parameters, selVector);
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
    if (function->compileFunc != nullptr) {
        function->compileFunc(bindData.get(), parameters, resultVector);
    }
}

} // namespace evaluator
} // namespace kuzu
