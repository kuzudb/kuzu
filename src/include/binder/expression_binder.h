#pragma once

#include "binder/expression/expression.h"
#include "catalog/catalog.h"
#include "common/types/value.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
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
    std::shared_ptr<Expression> bindBooleanExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindBooleanExpression(
        common::ExpressionType expressionType, const expression_vector& children);

    std::shared_ptr<Expression> bindComparisonExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindComparisonExpression(
        common::ExpressionType expressionType, const expression_vector& children);

    std::shared_ptr<Expression> bindNullOperatorExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> bindPropertyExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> bindNodePropertyExpression(
        const Expression& expression, const std::string& propertyName);
    std::shared_ptr<Expression> bindRelPropertyExpression(
        const Expression& expression, const std::string& propertyName);
    std::unique_ptr<Expression> createPropertyExpression(const Expression& nodeOrRel,
        const std::vector<catalog::Property>& propertyName, bool isPrimaryKey);

    std::shared_ptr<Expression> bindFunctionExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> bindScalarFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::shared_ptr<Expression> bindScalarFunctionExpression(
        const expression_vector& children, const std::string& functionName);
    std::shared_ptr<Expression> bindAggregateFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName,
        bool isDistinct);
    std::shared_ptr<Expression> staticEvaluate(
        const std::string& functionName, const expression_vector& children);

    std::shared_ptr<Expression> rewriteFunctionExpression(
        const parser::ParsedExpression& parsedExpression, const std::string& functionName);
    std::unique_ptr<Expression> createInternalNodeIDExpression(const Expression& node);
    std::shared_ptr<Expression> bindInternalIDExpression(std::shared_ptr<Expression> expression);
    std::shared_ptr<Expression> bindLabelFunction(const Expression& expression);
    std::unique_ptr<Expression> createInternalLengthExpression(const Expression& expression);
    std::shared_ptr<Expression> bindRecursiveJoinLengthFunction(const Expression& expression);

    std::shared_ptr<Expression> bindParameterExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> bindLiteralExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> createLiteralExpression(std::unique_ptr<common::Value> value);
    std::shared_ptr<Expression> createNullLiteralExpression();

    std::shared_ptr<Expression> bindVariableExpression(
        const parser::ParsedExpression& parsedExpression);
    std::shared_ptr<Expression> createVariableExpression(
        common::LogicalType logicalType, std::string uniqueName, std::string name);

    std::shared_ptr<Expression> bindExistentialSubqueryExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> bindCaseExpression(
        const parser::ParsedExpression& parsedExpression);

    /****** cast *****/
    // Note: we expose two implicitCastIfNecessary interfaces.
    // For function binding we cast with data type ID because function definition cannot be
    // recursively generated, e.g. list_extract(param) we only declare param with type LIST but do
    // not specify its child type.
    // For the rest, i.e. set clause binding, we cast with data type. For example, a.list = $1.
    static std::shared_ptr<Expression> implicitCastIfNecessary(
        const std::shared_ptr<Expression>& expression, const common::LogicalType& targetType);
    static std::shared_ptr<Expression> implicitCastIfNecessary(
        const std::shared_ptr<Expression>& expression, common::LogicalTypeID targetTypeID);
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
