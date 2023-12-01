#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/subquery_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_subquery_expression.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindSubqueryExpression(
    const ParsedExpression& parsedExpr) {
    auto& subqueryExpr = reinterpret_cast<const ParsedSubqueryExpression&>(parsedExpr);
    auto prevScope = binder->saveScope();
    auto [queryGraph, _] = binder->bindGraphPattern(subqueryExpr.getPatternElements());
    auto subqueryType = subqueryExpr.getSubqueryType();
    auto dataType =
        subqueryType == SubqueryType::COUNT ? LogicalType::INT64() : LogicalType::BOOL();
    auto rawName = subqueryExpr.getRawName();
    auto uniqueName = binder->getUniqueExpressionName(rawName);
    auto boundSubqueryExpr = make_shared<SubqueryExpression>(
        subqueryType, *dataType, std::move(queryGraph), uniqueName, std::move(rawName));
    // Bind predicate
    if (subqueryExpr.hasWhereClause()) {
        auto where = binder->bindWhereExpression(*subqueryExpr.getWhereClause());
        boundSubqueryExpr->setWhereExpression(std::move(where));
    }
    // Bind projection
    auto function = binder->catalog.getBuiltInFunctions()->matchAggregateFunction(
        COUNT_STAR_FUNC_NAME, std::vector<LogicalType*>{}, false);
    auto bindData = std::make_unique<FunctionBindData>(LogicalType(function->returnTypeID));
    auto countStarExpr = std::make_shared<AggregateFunctionExpression>(COUNT_STAR_FUNC_NAME,
        std::move(bindData), expression_vector{}, function->clone(),
        binder->getUniqueExpressionName(COUNT_STAR_FUNC_NAME));
    boundSubqueryExpr->setCountStarExpr(countStarExpr);
    std::shared_ptr<Expression> projectionExpr;
    switch (subqueryType) {
    case SubqueryType::COUNT: {
        // Rewrite COUNT subquery as COUNT(*)
        projectionExpr = countStarExpr;
    } break;
    case SubqueryType::EXISTS: {
        // Rewrite EXISTS subquery as COUNT(*) > 0
        auto literalExpr = createLiteralExpression(std::make_unique<Value>((int64_t)0));
        projectionExpr = bindComparisonExpression(
            ExpressionType::GREATER_THAN, expression_vector{countStarExpr, literalExpr});
    } break;
    default:
        KU_UNREACHABLE;
    }
    // Use the same unique identifier for projection & subquery expression. We will replace subquery
    // expression with projection expression during processing.
    projectionExpr->setUniqueName(uniqueName);
    boundSubqueryExpr->setProjectionExpr(projectionExpr);
    binder->restoreScope(std::move(prevScope));
    return boundSubqueryExpr;
}

} // namespace binder
} // namespace kuzu
