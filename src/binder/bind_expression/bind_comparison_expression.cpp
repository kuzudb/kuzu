#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(std::move(child));
    }
    return bindComparisonExpression(parsedExpression.getExpressionType(), std::move(children));
}

std::shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {
    auto builtInFunctions = binder->catalog.getBuiltInFunctions();
    auto functionName = expressionTypeToString(expressionType);
    std::vector<LogicalType*> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(&child->dataType);
    }
    auto function = reinterpret_cast<function::ScalarFunction*>(
        builtInFunctions->matchScalarFunction(functionName, childrenTypes));
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
    }
    auto bindData =
        std::make_unique<function::FunctionBindData>(LogicalType(function->returnTypeID));
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::createEqualityComparisonExpression(
    std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
    return bindComparisonExpression(
        ExpressionType::EQUALS, expression_vector{std::move(left), std::move(right)});
}

} // namespace binder
} // namespace kuzu
