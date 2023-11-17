#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression_visitor.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/string_format.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/cast/vector_cast_functions.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindExpression(
    const parser::ParsedExpression& parsedExpression) {
    std::shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.getExpressionType();
    if (isExpressionBoolConnection(expressionType)) {
        expression = bindBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionNullOperator(expressionType)) {
        expression = bindNullOperatorExpression(parsedExpression);
    } else if (ExpressionType::FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (ExpressionType::PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (ExpressionType::PARAMETER == expressionType) {
        expression = bindParameterExpression(parsedExpression);
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (ExpressionType::VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (ExpressionType::SUBQUERY == expressionType) {
        expression = bindSubqueryExpression(parsedExpression);
    } else if (ExpressionType::CASE_ELSE == expressionType) {
        expression = bindCaseExpression(parsedExpression);
    } else {
        throw NotImplementedException(
            "bindExpression(" + expressionTypeToString(expressionType) + ").");
    }
    if (parsedExpression.hasAlias()) {
        expression->setAlias(parsedExpression.getAlias());
    }
    if (isExpressionAggregate(expression->expressionType)) {
        validateAggregationExpressionIsNotNested(*expression);
    }
    if (ExpressionVisitor::needFold(*expression)) {
        return foldExpression(expression);
    }
    return expression;
}

std::shared_ptr<Expression> ExpressionBinder::foldExpression(
    std::shared_ptr<Expression> expression) {
    auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(
        expression, binder->memoryManager);
    auto result = createLiteralExpression(std::move(value));
    // Fold result should preserve the alias original expression. E.g.
    // RETURN 2, 1 + 1 AS x
    // Once folded, 1 + 1 will become 2 and have the same identifier as the first RETURN element.
    // We preserve alias (x) to avoid such conflict.
    if (expression->hasAlias()) {
        result->setAlias(expression->getAlias());
    } else {
        result->setAlias(expression->toString());
    }
    return result;
}

static std::string unsupportedImplicitCastException(
    const Expression& expression, const common::LogicalType& targetType) {
    return stringFormat(
        "Expression {} has data type {} but expected {}. Implicit cast is not supported.",
        expression.toString(), expression.dataType.toString(), targetType.toString());
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, common::LogicalTypeID targetTypeID) {
    if (LogicalTypeUtils::isNested(targetTypeID)) {
        if (expression->getDataType().getLogicalTypeID() == common::LogicalTypeID::ANY) {
            throw BinderException(stringFormat(
                "Cannot resolve recursive data type for expression {}.", expression->toString()));
        }
        // We don't support casting to nested data type. So instead we validate type match.
        if (expression->getDataType().getLogicalTypeID() != targetTypeID) {
            throw BinderException(
                unsupportedImplicitCastException(*expression, LogicalType{targetTypeID}));
        }
        return expression;
    }
    return implicitCastIfNecessary(expression, LogicalType(targetTypeID));
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    if (targetType.getLogicalTypeID() == LogicalTypeID::ANY || expression->dataType == targetType) {
        return expression;
    }
    if (expression->dataType.getLogicalTypeID() == LogicalTypeID::ANY) {
        resolveAnyDataType(*expression, targetType);
        return expression;
    }
    return implicitCast(expression, targetType);
}

std::shared_ptr<Expression> ExpressionBinder::implicitCast(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    if (CastFunction::hasImplicitCast(expression->dataType, targetType)) {
        auto functionName = stringFormat("CAST_TO({})", targetType.toString());
        auto children = expression_vector{expression};
        auto bindData = std::make_unique<FunctionBindData>(targetType);
        auto scalarFunction = CastFunction::bindCastFunction(
            functionName, expression->dataType.getLogicalTypeID(), targetType.getLogicalTypeID());
        auto uniqueName = ScalarFunctionExpression::getUniqueName(functionName, children);
        return std::make_shared<ScalarFunctionExpression>(functionName, ExpressionType::FUNCTION,
            std::move(bindData), std::move(children), scalarFunction->execFunc,
            nullptr /* selectFunc */, std::move(uniqueName));
    } else {
        throw BinderException(unsupportedImplicitCastException(*expression, targetType));
    }
}

void ExpressionBinder::resolveAnyDataType(Expression& expression, const LogicalType& targetType) {
    if (expression.expressionType == ExpressionType::PARAMETER) { // expression is parameter
        ((ParameterExpression&)expression).setDataType(targetType);
    } else { // expression is null literal
        KU_ASSERT(expression.expressionType == ExpressionType::LITERAL);
        ((LiteralExpression&)expression).setDataType(targetType);
    }
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const std::vector<LogicalTypeID>& targets) {
    auto dataType = expression.dataType;
    auto targetsSet = std::unordered_set<LogicalTypeID>{targets.begin(), targets.end()};
    if (!targetsSet.contains(dataType.getLogicalTypeID())) {
        throw BinderException(stringFormat("{} has data type {} but {} was expected.",
            expression.toString(), LogicalTypeUtils::toString(dataType.getLogicalTypeID()),
            LogicalTypeUtils::toString(targets)));
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.getNumChildren() == 0) {
        return;
    }
    if (ExpressionVisitor::hasAggregate(*expression.getChild(0))) {
        throw BinderException(
            stringFormat("Expression {} contains nested aggregation.", expression.toString()));
    }
}

} // namespace binder
} // namespace kuzu
