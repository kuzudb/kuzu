#pragma once

#include "function/scalar_function.h"
#include "null_function_executor.h"

namespace kuzu {
namespace function {

class VectorNullFunction {
public:
    static void bindExecFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_exec_func& func);

    static void bindSelectFunction(common::ExpressionType expressionType,
        const binder::expression_vector& children, scalar_select_func& func);

private:
    template<typename FUNC>
    static void UnaryNullExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        NullOperationExecutor::execute<FUNC>(*params[0], result);
    }

    template<typename FUNC>
    static bool UnaryNullSelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        KU_ASSERT(params.size() == 1);
        return NullOperationExecutor::select<FUNC>(*params[0], selVector);
    }
};

} // namespace function
} // namespace kuzu
