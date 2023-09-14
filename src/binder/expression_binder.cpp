#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression_visitor.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
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
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (PARAMETER == expressionType) {
        expression = bindParameterExpression(parsedExpression);
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        expression = bindExistentialSubqueryExpression(parsedExpression);
    } else if (CASE_ELSE == expressionType) {
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

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, LogicalTypeID targetTypeID) {
    if (targetTypeID == LogicalTypeID::ANY ||
        expression->dataType.getLogicalTypeID() == targetTypeID) {
        return expression;
    }
    if (expression->dataType.getLogicalTypeID() == LogicalTypeID::ANY) {
        if (targetTypeID == LogicalTypeID::VAR_LIST) {
            // e.g. len($1) we cannot infer the child type for $1.
            throw BinderException("Cannot resolve recursive data type for expression " +
                                  expression->toString() + ".");
        }
        resolveAnyDataType(*expression, LogicalType(targetTypeID));
        return expression;
    }
    assert(targetTypeID != LogicalTypeID::VAR_LIST);
    return implicitCast(expression, LogicalType(targetTypeID));
}

std::shared_ptr<Expression> ExpressionBinder::implicitCast(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    if (VectorCastFunction::hasImplicitCast(expression->dataType, targetType)) {
        auto functionName = VectorCastFunction::bindImplicitCastFuncName(targetType);
        auto children = expression_vector{expression};
        auto bindData = std::make_unique<FunctionBindData>(targetType);
        function::scalar_exec_func execFunc;
        VectorCastFunction::bindImplicitCastFunc(
            expression->dataType.getLogicalTypeID(), targetType.getLogicalTypeID(), execFunc);
        auto uniqueName = ScalarFunctionExpression::getUniqueName(functionName, children);
        return std::make_shared<ScalarFunctionExpression>(functionName, FUNCTION,
            std::move(bindData), std::move(children), execFunc, nullptr /* selectFunc */,
            std::move(uniqueName));
    } else {
        throw BinderException("Expression " + expression->toString() + " has data type " +
                              LogicalTypeUtils::dataTypeToString(expression->dataType) +
                              " but expect " + LogicalTypeUtils::dataTypeToString(targetType) +
                              ". Implicit cast is not supported.");
    }
}

void ExpressionBinder::resolveAnyDataType(Expression& expression, const LogicalType& targetType) {
    if (expression.expressionType == PARAMETER) { // expression is parameter
        ((ParameterExpression&)expression).setDataType(targetType);
    } else { // expression is null literal
        assert(expression.expressionType == LITERAL);
        ((LiteralExpression&)expression).setDataType(targetType);
    }
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const std::vector<LogicalTypeID>& targets) {
    auto dataType = expression.dataType;
    auto targetsSet = std::unordered_set<LogicalTypeID>{targets.begin(), targets.end()};
    if (!targetsSet.contains(dataType.getLogicalTypeID())) {
        throw BinderException(expression.toString() + " has data type " +
                              LogicalTypeUtils::dataTypeToString(dataType.getLogicalTypeID()) +
                              ". " + LogicalTypeUtils::dataTypesToString(targets) +
                              " was expected.");
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.getNumChildren() == 0) {
        return;
    }
    if (ExpressionVisitor::hasAggregate(*expression.getChild(0))) {
        throw BinderException(
            "Expression " + expression.toString() + " contains nested aggregation.");
    }
}

} // namespace binder
} // namespace kuzu
