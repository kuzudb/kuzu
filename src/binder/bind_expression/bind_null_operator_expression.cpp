#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "function/null/vector_null_operations.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindNullOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = function::VectorNullOperations::bindExecFunction(expressionType, children);
    auto selectFunc = function::VectorNullOperations::bindSelectFunction(expressionType, children);
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType,
        common::DataType(common::BOOL), std::move(children), std::move(execFunc),
        std::move(selectFunc), uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
