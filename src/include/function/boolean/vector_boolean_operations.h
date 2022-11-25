#pragma once

#include "boolean_operation_executor.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorBooleanOperations : public VectorOperations {

public:
    static scalar_exec_func bindExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindSelectFunction(
        ExpressionType expressionType, const expression_vector& children);

private:
    template<typename FUNC>
    static void BinaryBooleanExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryBooleanOperationExecutor::execute<FUNC>(*params[0], *params[1], result);
    }

    template<typename FUNC>
    static bool BinaryBooleanSelectFunction(
        const vector<shared_ptr<ValueVector>>& params, SelectionVector& selVector) {
        assert(params.size() == 2);
        return BinaryBooleanOperationExecutor::select<FUNC>(*params[0], *params[1], selVector);
    }

    template<typename FUNC>
    static void UnaryBooleanExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 1);
        UnaryBooleanOperationExecutor::execute<FUNC>(*params[0], result);
    }

    template<typename FUNC>
    static bool UnaryBooleanSelectFunction(
        const vector<shared_ptr<ValueVector>>& params, SelectionVector& selVector) {
        assert(params.size() == 1);
        return UnaryBooleanOperationExecutor::select<FUNC>(*params[0], selVector);
    }

    static scalar_exec_func bindBinaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindBinarySelectFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_exec_func bindUnaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindUnarySelectFunction(
        ExpressionType expressionType, const expression_vector& children);
};

} // namespace function
} // namespace kuzu
