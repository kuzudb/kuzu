#include "function/null/vector_null_functions.h"

#include "binder/expression/expression.h"
#include "common/assert.h"
#include "common/enums/expression_type.h"
#include "common/exception/runtime.h"
#include "function/null/null_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void VectorNullFunction::bindExecFunction(ExpressionType expressionType,
    const binder::expression_vector& /*children*/, scalar_exec_func& func) {
    switch (expressionType) {
    case ExpressionType::IS_NULL: {
        func = UnaryNullExecFunction<IsNull>;
        return;
    }
    case ExpressionType::IS_NOT_NULL: {
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
    KU_ASSERT(children.size() == 1);
    (void)children;
    switch (expressionType) {
    case ExpressionType::IS_NULL: {
        func = UnaryNullSelectFunction<IsNull>;
        return;
    }
    case ExpressionType::IS_NOT_NULL: {
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
