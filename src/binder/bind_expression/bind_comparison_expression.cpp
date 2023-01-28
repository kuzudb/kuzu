#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(std::move(child));
    }
    return bindComparisonExpression(parsedExpression.getExpressionType(), std::move(children));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    auto functionName = expressionTypeToString(expressionType);
    vector<DataType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(function->returnTypeID),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
