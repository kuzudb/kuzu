#include "function/null/vector_null_functions.h"

#include "function/null/null_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorNullFunction::bindExecFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_exec_func& func) {
    assert(children.size() == 1);
    switch (expressionType) {
    case IS_NULL: {
        func = UnaryNullExecFunction<IsNull>;
        return;
    }
    case IS_NOT_NULL: {
        func = UnaryNullExecFunction<IsNotNull>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               "for VectorNullOperations::bindUnaryExecFunction.");
    }
}

void VectorNullFunction::bindSelectFunction(ExpressionType expressionType,
    const binder::expression_vector& children, scalar_select_func& func) {
    assert(children.size() == 1);
    switch (expressionType) {
    case IS_NULL: {
        func = UnaryNullSelectFunction<IsNull>;
        return;
    }
    case IS_NOT_NULL: {
        func = UnaryNullSelectFunction<IsNotNull>;
        return;
    }
    default:
        throw RuntimeException("Invalid expression type " + expressionTypeToString(expressionType) +
                               "for VectorNullOperations::bindUnarySelectFunction.");
    }
}

} // namespace function
} // namespace kuzu
