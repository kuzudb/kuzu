#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "function/null/vector_null_functions.h"

using namespace kuzu::common;
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
    function::scalar_exec_func execFunc;
    function::VectorNullFunction::bindExecFunction(expressionType, children, execFunc);
    function::scalar_select_func selectFunc;
    function::VectorNullFunction::bindSelectFunction(expressionType, children, selectFunc);
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType(LogicalTypeID::BOOL));
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(children), std::move(execFunc), std::move(selectFunc), uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
