#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "common/type_utils.h"
#include "function/cast/vector_cast_operations.h"

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
    return expression;
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, const DataType& targetType) {
    if (targetType.typeID == ANY || expression->dataType == targetType) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        resolveAnyDataType(*expression, targetType);
        return expression;
    }
    return implicitCast(expression, targetType);
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (targetTypeID == ANY || expression->dataType.typeID == targetTypeID) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        if (targetTypeID == VAR_LIST) {
            // e.g. len($1) we cannot infer the child type for $1.
            throw BinderException("Cannot resolve recursive data type for expression " +
                                  expression->toString() + ".");
        }
        resolveAnyDataType(*expression, DataType(targetTypeID));
        return expression;
    }
    assert(targetTypeID != VAR_LIST);
    return implicitCast(expression, DataType(targetTypeID));
}

std::shared_ptr<Expression> ExpressionBinder::implicitCast(
    const std::shared_ptr<Expression>& expression, const common::DataType& targetType) {
    if (VectorCastOperations::hasImplicitCast(expression->dataType, targetType)) {
        auto functionName = VectorCastOperations::bindImplicitCastFuncName(targetType);
        auto children = expression_vector{expression};
        auto uniqueName = ScalarFunctionExpression::getUniqueName(functionName, children);
        return std::make_shared<ScalarFunctionExpression>(functionName, FUNCTION,
            DataType{targetType.typeID}, std::move(children),
            VectorCastOperations::bindImplicitCastFunc(
                expression->dataType.typeID, targetType.typeID),
            nullptr /* selectFunc */, std::move(uniqueName));
    } else {
        throw common::BinderException("Expression " + expression->toString() + " has data type " +
                                      common::Types::dataTypeToString(expression->dataType) +
                                      " but expect " + common::Types::dataTypeToString(targetType) +
                                      ". Implicit cast is not supported.");
    }
}

void ExpressionBinder::resolveAnyDataType(Expression& expression, const DataType& targetType) {
    if (expression.expressionType == PARAMETER) { // expression is parameter
        ((ParameterExpression&)expression).setDataType(targetType);
    } else { // expression is null literal
        assert(expression.expressionType == LITERAL);
        ((LiteralExpression&)expression).setDataType(targetType);
    }
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const std::unordered_set<DataTypeID>& targets) {
    auto dataType = expression.dataType;
    if (!targets.contains(dataType.typeID)) {
        std::vector<DataTypeID> targetsVec{targets.begin(), targets.end()};
        throw BinderException(expression.toString() + " has data type " +
                              Types::dataTypeToString(dataType.typeID) + ". " +
                              Types::dataTypesToString(targetsVec) + " was expected.");
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.getNumChildren() == 0) {
        return;
    }
    if (expression.getChild(0)->hasAggregationExpression()) {
        throw BinderException(
            "Expression " + expression.toString() + " contains nested aggregation.");
    }
}

} // namespace binder
} // namespace kuzu
