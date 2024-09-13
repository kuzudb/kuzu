#pragma once

#include "binder/expression/expression.h"
#include "common/types/value/value.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace binder {

class Binder;
struct CaseAlternative;

class ExpressionBinder {
    friend class Binder;

public:
    ExpressionBinder(Binder* queryBinder, main::ClientContext* context)
        : binder{queryBinder}, context{context} {}

    std::shared_ptr<Expression> bindExpression(const parser::ParsedExpression& parsedExpression);

    // TODO(Xiyang): move to an expression rewriter
    std::shared_ptr<Expression> foldExpression(const std::shared_ptr<Expression>& expression);

    // Boolean expressions.
    std::shared_ptr<Expression> bindBooleanExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindBooleanExpression(common::ExpressionType expressionType,
        const expression_vector& children);

    std::shared_ptr<Expression> combineBooleanExpressions(common::ExpressionType expressionType,
        std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);
    // Comparison expressions.
    std::shared_ptr<Expression> bindComparisonExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindComparisonExpression(common::ExpressionType expressionType,
        const expression_vector& children);
    std::shared_ptr<Expression> createEqualityComparisonExpression(std::shared_ptr<Expression> left,
        std::shared_ptr<Expression> right);
    // Null operator expressions.
    std::shared_ptr<Expression> bindNullOperatorExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindNullOperatorExpression(common::ExpressionType expressionType,
        const expression_vector& children);

    // Property expressions.
    expression_vector bindPropertyStarExpression(const parser::ParsedExpression& parsedExpression);
    expression_vector bindNodeOrRelPropertyStarExpression(const Expression& child);
    expression_vector bindStructPropertyStarExpression(const std::shared_ptr<Expression>& child);
    std::shared_ptr<Expression> bindPropertyExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindNodeOrRelPropertyExpression(const Expression& child,
        const std::string& propertyName);
    std::shared_ptr<Expression> bindStructPropertyExpression(std::shared_ptr<Expression> child,
        const std::string& propertyName);
    // Function expressions.
    std::shared_ptr<Expression> bindFunctionExpression(
        const parser::ParsedExpression& parsedExpression);
    void bindLambdaExpression(const Expression& lambdaInput, Expression& lambdaExpr);
    std::shared_ptr<Expression> bindLambdaExpression(
        const parser::ParsedExpression& parsedExpression) const;

    std::shared_ptr<Expression> bindScalarFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::shared_ptr<Expression> bindScalarFunctionExpression(const expression_vector& children,
        const std::string& functionName);
    std::shared_ptr<Expression> bindRewriteFunctionExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindAggregateFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName,
        bool isDistinct);
    std::shared_ptr<Expression> bindMacroExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& macroName);

    std::shared_ptr<Expression> rewriteFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::shared_ptr<Expression> bindStartNodeExpression(const Expression& expression);
    std::shared_ptr<Expression> bindEndNodeExpression(const Expression& expression);
    std::shared_ptr<Expression> bindLabelFunction(const Expression& expression);

    // Parameter expressions.
    std::shared_ptr<Expression> bindParameterExpression(
        const parser::ParsedExpression& parsedExpression);
    // Literal expressions.
    std::shared_ptr<Expression> bindLiteralExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> createLiteralExpression(const common::Value& value);
    std::shared_ptr<Expression> createLiteralExpression(const std::string& strVal);
    std::shared_ptr<Expression> createNullLiteralExpression();
    std::shared_ptr<Expression> createNullLiteralExpression(const common::Value& value);
    // Variable expressions.
    std::shared_ptr<Expression> bindVariableExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindVariableExpression(const std::string& varName);
    std::shared_ptr<Expression> createVariableExpression(common::LogicalType logicalType,
        std::string_view name);
    std::shared_ptr<Expression> createVariableExpression(common::LogicalType logicalType,
        std::string name);
    // Subquery expressions.
    std::shared_ptr<Expression> bindSubqueryExpression(
        const parser::ParsedExpression& parsedExpression);
    // Case expressions.
    std::shared_ptr<Expression> bindCaseExpression(
        const parser::ParsedExpression& parsedExpression);

    /****** cast *****/
    std::shared_ptr<Expression> implicitCastIfNecessary(
        const std::shared_ptr<Expression>& expression, const common::LogicalType& targetType);
    // Use implicitCast to cast to types you have obtained through known implicit casting rules.
    // Use forceCast to cast to types you have obtained through other means, for example,
    // through a maxLogicalType function
    std::shared_ptr<Expression> implicitCast(const std::shared_ptr<Expression>& expression,
        const common::LogicalType& targetType);
    std::shared_ptr<Expression> forceCast(const std::shared_ptr<Expression>& expression,
        const common::LogicalType& targetType);

    std::string getUniqueName(const std::string& name) const;

private:
    Binder* binder;
    main::ClientContext* context;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
    bool bindOrderByAfterAggregation = false;
};

} // namespace binder
} // namespace kuzu
