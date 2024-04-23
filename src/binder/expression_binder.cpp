#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression_visitor.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/string_format.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/cast/vector_cast_functions.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

static void validateAggregationExpressionIsNotNested(const Expression& expression);

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
    const std::shared_ptr<Expression>& expression) {
    auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(expression,
        context->getMemoryManager());
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

static std::string unsupportedImplicitCastException(const Expression& expression,
    const std::string& targetTypeStr) {
    return stringFormat(
        "Expression {} has data type {} but expected {}. Implicit cast is not supported.",
        expression.toString(), expression.dataType.toString(), targetTypeStr);
}

static bool compatible(const LogicalType& type, const LogicalType& target) {
    if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
        return true;
    }
    if (type.getLogicalTypeID() != target.getLogicalTypeID()) {
        return false;
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        return compatible(*ListType::getChildType(&type), *ListType::getChildType(&target));
    }
    case LogicalTypeID::ARRAY: {
        return compatible(*ArrayType::getChildType(&type), *ArrayType::getChildType(&target));
    }
    case LogicalTypeID::STRUCT: {
        if (StructType::getNumFields(&type) != StructType::getNumFields(&target)) {
            return false;
        }
        for (auto i = 0u; i < StructType::getNumFields(&type); ++i) {
            if (!compatible(*StructType::getField(&type, i)->getType(),
                    *StructType::getField(&target, i)->getType())) {
                return false;
            }
        }
        return true;
    }
    case LogicalTypeID::RDF_VARIANT:
    case LogicalTypeID::UNION:
    case LogicalTypeID::MAP:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
        return false;
    default:
        return true;
    }
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    if (expression->dataType == targetType || targetType.containsAny()) { // No need to cast.
        return expression;
    }
    if (compatible(expression->getDataType(), targetType)) {
        expression->cast(targetType);
        return expression;
    }
    return implicitCast(expression, targetType);
}

std::shared_ptr<Expression> ExpressionBinder::implicitCast(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    if (CastFunction::hasImplicitCast(expression->dataType, targetType)) {
        return forceCast(expression, targetType);
    } else {
        throw BinderException(unsupportedImplicitCastException(*expression, targetType.toString()));
    }
}

// cast without implicit checking.
std::shared_ptr<Expression> ExpressionBinder::forceCast(
    const std::shared_ptr<Expression>& expression, const LogicalType& targetType) {
    auto functionName = "CAST";
    auto children = expression_vector{expression,
        createLiteralExpression(std::make_unique<Value>(targetType.toString()))};
    return bindScalarFunctionExpression(children, functionName);
}

void validateAggregationExpressionIsNotNested(const Expression& expression) {
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
