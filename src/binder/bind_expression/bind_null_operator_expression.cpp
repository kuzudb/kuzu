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
        auto boundChild = bindExpression(*parsedExpression.getChild(i));
        if (boundChild->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            // e.g. NULL IS NULL. We assign a default type to the expression.
            boundChild = implicitCastIfNecessary(std::move(boundChild), LogicalType::BOOL());
        }
        children.push_back(std::move(boundChild));
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = ExpressionTypeUtil::toString(expressionType);
    function::scalar_func_exec_t execFunc;
    function::VectorNullFunction::bindExecFunction(expressionType, children, execFunc);
    function::scalar_func_select_t selectFunc;
    function::VectorNullFunction::bindSelectFunction(expressionType, children, selectFunc);
    auto bindData = std::make_unique<function::FunctionBindData>(LogicalType::BOOL());
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(children), std::move(execFunc), std::move(selectFunc), uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
