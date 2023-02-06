#pragma once

#include "function/vector_operations.h"
#include "null_operation_executor.h"

namespace kuzu {
namespace function {

class VectorNullOperations : public VectorOperations {

public:
    static scalar_exec_func bindExecFunction(
        common::ExpressionType expressionType, const binder::expression_vector& children);

    static scalar_select_func bindSelectFunction(
        common::ExpressionType expressionType, const binder::expression_vector& children);

private:
    template<typename FUNC>
    static void UnaryNullExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        NullOperationExecutor::execute<FUNC>(*params[0], result);
    }

    template<typename FUNC>
    static bool UnaryNullSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        assert(params.size() == 1);
        return NullOperationExecutor::select<FUNC>(*params[0], selVector);
    }

    static scalar_exec_func bindUnaryExecFunction(
        common::ExpressionType expressionType, const binder::expression_vector& children);

    static scalar_select_func bindUnarySelectFunction(
        common::ExpressionType expressionType, const binder::expression_vector& children);
};

} // namespace function
} // namespace kuzu
