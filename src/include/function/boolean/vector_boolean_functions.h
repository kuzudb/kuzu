#pragma once

#include "boolean_function_executor.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

class VectorBooleanFunction {
public:
    static void bindExecFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_exec_func& func);

    static void bindSelectFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_select_func& func);

private:
    template<typename FUNC>
    static void BinaryBooleanExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryBooleanFunctionExecutor::execute<FUNC>(*params[0], *params[1], result);
    }

    template<typename FUNC>
    static bool BinaryBooleanSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        assert(params.size() == 2);
        return BinaryBooleanFunctionExecutor::select<FUNC>(*params[0], *params[1], selVector);
    }

    template<typename FUNC>
    static void UnaryBooleanExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryBooleanOperationExecutor::execute<FUNC>(*params[0], result);
    }

    template<typename FUNC>
    static bool UnaryBooleanSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        assert(params.size() == 1);
        return UnaryBooleanOperationExecutor::select<FUNC>(*params[0], selVector);
    }

    static void bindBinaryExecFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_exec_func& func);

    static void bindBinarySelectFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_select_func& func);

    static void bindUnaryExecFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_exec_func& func);

    static void bindUnarySelectFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_select_func& func);
};

} // namespace function
} // namespace kuzu
