#include "function/null/vector_null_operations.h"

#include "function/null/null_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorNullOperations::bindExecFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_exec_func& func) {
    assert(children.size() == 1);
    switch (expressionType) {
    case IS_NULL: {
        func = UnaryNullExecFunction<operation::IsNull>;
        return;
    }
    case IS_NOT_NULL: {
        func = UnaryNullExecFunction<operation::IsNotNull>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               "for VectorNullOperations::bindUnaryExecFunction.");
    }
}

void VectorNullOperations::bindSelectFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_select_func& func) {
    assert(children.size() == 1);
    switch (expressionType) {
    case IS_NULL: {
        func = UnaryNullSelectFunction<operation::IsNull>;
        return;
    }
    case IS_NOT_NULL: {
        func = UnaryNullSelectFunction<operation::IsNotNull>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               "for VectorNullOperations::bindUnarySelectFunction.");
    }
}

} // namespace function
} // namespace kuzu
