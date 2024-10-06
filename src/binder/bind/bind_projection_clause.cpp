#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_visitor.h"
#include "binder/query/return_with_clause/bound_return_clause.h"
#include "binder/query/return_with_clause/bound_with_clause.h"
#include "common/exception/binder.h"
#include "parser/expression/parsed_property_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

void validateColumnNamesAreUnique(const std::vector<std::string>& columnNames) {
    auto existColumnNames = std::unordered_set<std::string>();
    for (auto& name : columnNames) {
        if (existColumnNames.contains(name)) {
            throw BinderException(
                "Multiple result column with the same name " + name + " are not supported.");
        }
        existColumnNames.insert(name);
    }
}

std::vector<std::string> getColumnNames(const expression_vector& exprs,
    const std::vector<std::string>& aliases) {
    std::vector<std::string> columnNames;
    for (auto i = 0u; i < exprs.size(); ++i) {
        if (aliases[i].empty()) {
            columnNames.push_back(exprs[i]->toString());
        } else {
            columnNames.push_back(aliases[i]);
        }
    }
    return columnNames;
}

BoundWithClause Binder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto [projectionExprs, aliases] = bindProjectionList(*projectionBody);
    // Check all expressions are aliased
    for (auto& alias : aliases) {
        if (alias.empty()) {
            throw BinderException("Expression in WITH must be aliased (use AS).");
        }
    }
    auto columnNames = getColumnNames(projectionExprs, aliases);
    validateColumnNamesAreUnique(columnNames);
    auto boundProjectionBody = bindProjectionBody(*projectionBody, projectionExprs, aliases);
    validateOrderByFollowedBySkipOrLimitInWithClause(boundProjectionBody);
    // Update scope
    scope.clear();
    for (auto i = 0u; i < projectionExprs.size(); ++i) {
        addToScope(aliases[i], projectionExprs[i]);
    }
    auto boundWithClause = BoundWithClause(std::move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause.setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

BoundReturnClause Binder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto [projectionExprs, aliases] = bindProjectionList(*projectionBody);
    auto columnNames = getColumnNames(projectionExprs, aliases);
    validateColumnNamesAreUnique(columnNames);
    auto boundProjectionBody = bindProjectionBody(*projectionBody, projectionExprs, aliases);
    auto statementResult = BoundStatementResult();
    KU_ASSERT(columnNames.size() == projectionExprs.size());
    for (auto i = 0u; i < columnNames.size(); ++i) {
        statementResult.addColumn(columnNames[i], projectionExprs[i]);
    }
    return BoundReturnClause(std::move(boundProjectionBody), std::move(statementResult));
}

static expression_vector getAggregateExpressions(const std::shared_ptr<Expression>& expression,
    const BinderScope& scope) {
    expression_vector result;
    if (expression->hasAlias() && scope.contains(expression->getAlias())) {
        return result;
    }
    if (expression->expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        result.push_back(expression);
        return result;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
        for (auto& expr : getAggregateExpressions(child, scope)) {
            result.push_back(expr);
        }
    }
    return result;
}

std::pair<expression_vector, std::vector<std::string>> Binder::bindProjectionList(
    const ProjectionBody& projectionBody) {
    expression_vector projectionExprs;
    std::vector<std::string> aliases;
    for (auto& parsedExpr : projectionBody.getProjectionExpressions()) {
        if (parsedExpr->getExpressionType() == ExpressionType::STAR) {
            // Rewrite star expression as all expression in scope.
            if (scope.empty()) {
                throw BinderException(
                    "RETURN or WITH * is not allowed when there are no variables in scope.");
            }
            for (auto& expr : scope.getExpressions()) {
                projectionExprs.push_back(expr);
                aliases.push_back(expr->getAlias());
            }
        } else if (parsedExpr->getExpressionType() == ExpressionType::PROPERTY) {
            auto& propExpr = parsedExpr->constCast<ParsedPropertyExpression>();
            if (propExpr.isStar()) {
                // Rewrite property star expression
                for (auto& expr : expressionBinder.bindPropertyStarExpression(*parsedExpr)) {
                    projectionExprs.push_back(expr);
                    aliases.push_back("");
                }
            } else {
                auto expr = expressionBinder.bindExpression(*parsedExpr);
                projectionExprs.push_back(expr);
                aliases.push_back(parsedExpr->getAlias());
            }
        } else {
            auto expr = expressionBinder.bindExpression(*parsedExpr);
            projectionExprs.push_back(expr);
            aliases.push_back(parsedExpr->hasAlias() ? parsedExpr->getAlias() : expr->getAlias());
        }
    }
    return {projectionExprs, aliases};
}

static void validateNestedAggregate(const Expression& expr, const BinderScope& scope) {
    KU_ASSERT(expr.expressionType == ExpressionType::AGGREGATE_FUNCTION);
    if (expr.getNumChildren() == 0) { // Skip COUNT(*)
        return;
    }
    auto collector = AggregateExprCollector();
    collector.visit(expr.getChild(0));
    for (auto& agg : collector.getAggregates()) {
        if (!scope.contains(agg->getAlias())) {
            throw BinderException(
                stringFormat("Expression {} contains nested aggregation.", expr.toString()));
        }
    }
}

BoundProjectionBody Binder::bindProjectionBody(const parser::ProjectionBody& projectionBody,
    const expression_vector& projectionExprs, const std::vector<std::string>& aliases) {
    expression_vector groupByExprs;
    expression_vector aggregateExprs;
    KU_ASSERT(projectionExprs.size() == aliases.size());
    for (auto i = 0u; i < projectionExprs.size(); ++i) {
        auto expr = projectionExprs[i];
        auto aggExprs = getAggregateExpressions(expr, scope);
        if (!aggExprs.empty()) {
            for (auto& agg : aggExprs) {
                aggregateExprs.push_back(agg);
            }
        } else {
            groupByExprs.push_back(expr);
        }
        expr->setAlias(aliases[i]);
    }

    auto boundProjectionBody = BoundProjectionBody(projectionBody.getIsDistinct());
    boundProjectionBody.setProjectionExpressions(projectionExprs);

    if (!aggregateExprs.empty()) {
        for (auto& expr : aggregateExprs) {
            validateNestedAggregate(*expr, scope);
        }
        if (!groupByExprs.empty()) {
            // TODO(Xiyang): we can remove augment group by. But make sure we test sufficient
            // including edge case and bug before release.
            expression_vector augmentedGroupByExpressions = groupByExprs;
            for (auto& expression : groupByExprs) {
                if (ExpressionUtil::isNodePattern(*expression)) {
                    auto& node = expression->constCast<NodeExpression>();
                    augmentedGroupByExpressions.push_back(node.getInternalID());
                } else if (ExpressionUtil::isRelPattern(*expression)) {
                    auto& rel = expression->constCast<RelExpression>();
                    augmentedGroupByExpressions.push_back(rel.getInternalIDProperty());
                }
            }
            boundProjectionBody.setGroupByExpressions(std::move(augmentedGroupByExpressions));
        }
        boundProjectionBody.setAggregateExpressions(std::move(aggregateExprs));
    }
    // Bind order by
    if (projectionBody.hasOrderByExpressions()) {
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        expression_vector orderByExprs;
        if (boundProjectionBody.hasAggregateExpressions() || boundProjectionBody.isDistinct()) {
            scope.clear();
            KU_ASSERT(projectionBody.getProjectionExpressions().size() == projectionExprs.size());
            std::vector<std::string> tmpAliases;
            for (auto& expr : projectionBody.getProjectionExpressions()) {
                tmpAliases.push_back(expr->hasAlias() ? expr->getAlias() : expr->toString());
            }
            addToScope(tmpAliases, projectionExprs);
            expressionBinder.bindOrderByAfterAggregation = true;
            orderByExprs = bindOrderByExpressions(projectionBody.getOrderByExpressions());
            expressionBinder.bindOrderByAfterAggregation = false;
        } else {
            addToScope(aliases, projectionExprs);
            orderByExprs = bindOrderByExpressions(projectionBody.getOrderByExpressions());
        }
        boundProjectionBody.setOrderByExpressions(std::move(orderByExprs),
            projectionBody.getSortOrders());
    }
    // Bind skip
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody.setSkipNumber(
            bindSkipLimitExpression(*projectionBody.getSkipExpression()));
    }
    // Bind limit
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody.setLimitNumber(
            bindSkipLimitExpression(*projectionBody.getLimitExpression()));
    }
    return boundProjectionBody;
}

expression_vector Binder::bindOrderByExpressions(
    const std::vector<std::unique_ptr<ParsedExpression>>& parsedExprs) {
    expression_vector exprs;
    for (auto& parsedExpr : parsedExprs) {
        auto expr = expressionBinder.bindExpression(*parsedExpr);
        if (expr->dataType.getLogicalTypeID() == LogicalTypeID::NODE ||
            expr->dataType.getLogicalTypeID() == LogicalTypeID::REL) {
            throw BinderException(
                "Cannot order by " + expr->toString() + ". Order by node or rel is not supported.");
        }
        exprs.push_back(std::move(expr));
    }
    return exprs;
}

uint64_t Binder::bindSkipLimitExpression(const ParsedExpression& expression) {
    auto boundExpression = expressionBinder.bindExpression(expression);
    auto errorMsg = "The number of rows to skip/limit must be a non-negative integer.";
    if (boundExpression->expressionType != ExpressionType::LITERAL) {
        throw BinderException(errorMsg);
    }
    auto& literalExpr = boundExpression->constCast<LiteralExpression>();
    auto value = literalExpr.getValue();
    int64_t num = 0;
    // TODO: replace the following switch with value.cast()
    switch (value.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        num = value.getValue<int64_t>();
    } break;
    case LogicalTypeID::INT32: {
        num = value.getValue<int32_t>();
    } break;
    case LogicalTypeID::INT16: {
        num = value.getValue<int16_t>();
    } break;
    default:
        throw BinderException(errorMsg);
    }
    if (num < 0) {
        throw BinderException(errorMsg);
    }
    return num;
}

} // namespace binder
} // namespace kuzu
