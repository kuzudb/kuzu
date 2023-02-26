#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "function/boolean/vector_boolean_operations.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    return bindBooleanExpression(parsedExpression.getExpressionType(), children);
}

std::shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    ExpressionType expressionType, const expression_vector& children) {
    expression_vector childrenAfterCast;
    for (auto& child : children) {
        childrenAfterCast.push_back(implicitCastIfNecessary(child, BOOL));
    }
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc =
        function::VectorBooleanOperations::bindExecFunction(expressionType, childrenAfterCast);
    auto selectFunc =
        function::VectorBooleanOperations::bindSelectFunction(expressionType, childrenAfterCast);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(functionName, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, DataType(BOOL),
        std::move(childrenAfterCast), std::move(execFunc), std::move(selectFunc),
        uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
