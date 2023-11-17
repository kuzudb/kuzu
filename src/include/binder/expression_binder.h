#pragma once

#include "binder/expression/expression.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace common {
class Value;
}

namespace binder {

class Binder;
class CaseAlternative;

class ExpressionBinder {
    friend class Binder;

public:
    explicit ExpressionBinder(Binder* queryBinder) : binder{queryBinder} {}

    std::shared_ptr<Expression> bindExpression(const parser::ParsedExpression& parsedExpression);

    static void resolveAnyDataType(Expression& expression, const common::LogicalType& targetType);

private:
    // TODO(Xiyang): move to an expression rewriter
    std::shared_ptr<Expression> foldExpression(std::shared_ptr<Expression> expression);

    // Boolean expressions.
    std::shared_ptr<Expression> bindBooleanExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindBooleanExpression(
        common::ExpressionType expressionType, const expression_vector& children);
    std::shared_ptr<Expression> combineBooleanExpressions(common::ExpressionType expressionType,
        std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
    // Comparison expressions.
    std::shared_ptr<Expression> bindComparisonExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindComparisonExpression(
        common::ExpressionType expressionType, const expression_vector& children);
    std::shared_ptr<Expression> createEqualityComparisonExpression(
        std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
    // Null operator expressions.
    std::shared_ptr<Expression> bindNullOperatorExpression(
        const parser::ParsedExpression& parsedExpression);
    // Property expressions.
    expression_vector bindPropertyStarExpression(const parser::ParsedExpression& parsedExpression);
    expression_vector bindNodeOrRelPropertyStarExpression(const Expression& child);
    expression_vector bindStructPropertyStarExpression(std::shared_ptr<Expression> child);
    std::shared_ptr<Expression> bindPropertyExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindNodeOrRelPropertyExpression(
        const Expression& child, const std::string& propertyName);
    std::shared_ptr<Expression> bindStructPropertyExpression(
        std::shared_ptr<Expression> child, const std::string& propertyName);
    // Function expressions.
    std::shared_ptr<Expression> bindFunctionExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> bindScalarFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::shared_ptr<Expression> bindScalarFunctionExpression(
        const expression_vector& children, const std::string& functionName);
    std::shared_ptr<Expression> bindAggregateFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName,
        bool isDistinct);
    std::shared_ptr<Expression> bindMacroExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& macroName);

    std::shared_ptr<Expression> rewriteFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::unique_ptr<Expression> createInternalNodeIDExpression(const Expression& node);
    std::shared_ptr<Expression> bindInternalIDExpression(std::shared_ptr<Expression> expression);
    std::shared_ptr<Expression> bindLabelFunction(const Expression& expression);
    std::unique_ptr<Expression> createInternalLengthExpression(const Expression& expression);
    std::shared_ptr<Expression> bindRecursiveJoinLengthFunction(const Expression& expression);
    // Parameter expressions.
    std::shared_ptr<Expression> bindParameterExpression(
        const parser::ParsedExpression& parsedExpression);
    // Literal expressions.
    std::shared_ptr<Expression> bindLiteralExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> createLiteralExpression(std::unique_ptr<common::Value> value);
    std::shared_ptr<Expression> createStringLiteralExpression(const std::string& strVal);
    std::shared_ptr<Expression> createNullLiteralExpression();
    // Variable expressions.
    std::shared_ptr<Expression> bindVariableExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> createVariableExpression(
        common::LogicalType logicalType, std::string name);
    // Subquery expressions.
    std::shared_ptr<Expression> bindSubqueryExpression(
        const parser::ParsedExpression& parsedExpression);
    // Case expressions.
    std::shared_ptr<Expression> bindCaseExpression(
        const parser::ParsedExpression& parsedExpression);

    /****** cast *****/
    static std::shared_ptr<Expression> implicitCastIfNecessary(
        const std::shared_ptr<Expression>& expression, common::LogicalTypeID targetTypeID);
    static std::shared_ptr<Expression> implicitCastIfNecessary(
        const std::shared_ptr<Expression>& expression, const common::LogicalType& targetType);
    static std::shared_ptr<Expression> implicitCast(
        const std::shared_ptr<Expression>& expression, const common::LogicalType& targetType);

    /****** validation *****/
    static void validateExpectedDataType(
        const Expression& expression, common::LogicalTypeID target) {
        validateExpectedDataType(expression, std::vector<common::LogicalTypeID>{target});
    }
    static void validateExpectedDataType(
        const Expression& expression, const std::vector<common::LogicalTypeID>& targets);
    // E.g. SUM(SUM(a.age)) is not allowed
    static void validateAggregationExpressionIsNotNested(const Expression& expression);

private:
    Binder* binder;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
};

} // namespace binder
} // namespace kuzu
