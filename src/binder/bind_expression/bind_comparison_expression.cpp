#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"

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
    common::ExpressionType expressionType, const expression_vector& children) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    auto functionName = expressionTypeToString(expressionType);
    std::vector<common::LogicalType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
    }
    auto bindData =
        std::make_unique<function::FunctionBindData>(common::LogicalType(function->returnTypeID));
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
