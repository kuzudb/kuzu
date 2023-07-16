#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "function/boolean/vector_boolean_functions.h"

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
        childrenAfterCast.push_back(implicitCastIfNecessary(child, LogicalTypeID::BOOL));
    }
    auto functionName = expressionTypeToString(expressionType);
    function::scalar_exec_func execFunc;
    function::VectorBooleanFunction::bindExecFunction(expressionType, childrenAfterCast, execFunc);
    function::scalar_select_func selectFunc;
    function::VectorBooleanFunction::bindSelectFunction(
        expressionType, childrenAfterCast, selectFunc);
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType(LogicalTypeID::BOOL));
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(functionName, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(childrenAfterCast), std::move(execFunc), std::move(selectFunc),
        uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::combineConjunctiveExpressions(
    std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
    if (left == nullptr) {
        return right;
    } else if (right == nullptr) {
        return left;
    } else {
        return bindBooleanExpression(AND, expression_vector{std::move(left), std::move(right)});
    }
}

} // namespace binder
} // namespace kuzu
