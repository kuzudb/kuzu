#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"

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
    return bindComparisonExpression(parsedExpression.getExpressionType(), children);
}

std::shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {
    auto functions = context->getCatalog()->getFunctions(binder->clientContext->getTx());
    auto functionName = expressionTypeToString(expressionType);
    std::vector<LogicalType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
    auto function = ku_dynamic_cast<function::Function*, function::ScalarFunction*>(
        function::BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, functions));
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        if (LogicalTypeUtils::isNested(function->parameterTypeIDs[i]) &&
            children[i]->dataType.getLogicalTypeID() == LogicalTypeID::ANY) {
            // try matching the type to any other children
            bool foundValidChild = false;
            for (auto j = 0u; !foundValidChild && j < children.size(); ++j) {
                if (children[j]->dataType.getLogicalTypeID() == function->parameterTypeIDs[i]) {
                    childrenAfterCast.push_back(
                        implicitCastIfNecessary(children[i], children[j]->dataType));
                    foundValidChild = true;
                }
            }
            // LCOV_EXCL_START
            if (!foundValidChild) {
                throw common::BinderException(
                    stringFormat("Cannot resolve recursive data type for expression {}.",
                        children[i]->toString()));
            }
            // LCOV_EXCL_STOP
        } else {
            childrenAfterCast.push_back(
                implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
        }
    }
    auto bindData = std::make_unique<function::FunctionBindData>(
        std::make_unique<LogicalType>(function->returnTypeID));
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, expressionType, std::move(bindData),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::createEqualityComparisonExpression(
    std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
    return bindComparisonExpression(ExpressionType::EQUALS,
        expression_vector{std::move(left), std::move(right)});
}

} // namespace binder
} // namespace kuzu
