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

// WITH clause is like SQL CTE. So the projection list of WITH clause should be explicitly
// evaluated. This, however, creates problem in the following case
// MATCH (a) WITH a RETURN a.age;
// Although only a.age is needed for further processing. The CTE "MATCH (a) WITH a" require us to
// fully materialize all columns of "a". Note that we cannot rely on projection push down to
// optimize this because projection pushdown assumes all columns in WITH/RETURN are needed.
// Our solution is:
// First rewrite node and rel as their INTERNAL ID property in WITH clause. So
// MATCH (a) WITH a._id RETURN a.age;
// And then apply WithClauseProjectionRewriter after binding to rewrite as
// MATCH (a) WITH a._id, a.age RETURN a.age
static expression_vector rewriteProjectionInWithClause(const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (ExpressionUtil::isNodePattern(*expression)) {
            auto node = (NodeExpression*)expression.get();
            result.push_back(node->getInternalID());
        } else if (ExpressionUtil::isRelPattern(*expression)) {
            auto rel = (RelExpression*)expression.get();
            result.push_back(rel->getSrcNode()->getInternalID());
            result.push_back(rel->getDstNode()->getInternalID());
            result.push_back(rel->getInternalIDProperty());
        } else if (ExpressionUtil::isRecursiveRelPattern(*expression)) {
            auto rel = (RelExpression*)expression.get();
            result.push_back(expression);
            result.push_back(rel->getLengthExpression());
        } else {
            result.push_back(expression);
        }
    }
    return ExpressionUtil::removeDuplication(result);
}

std::unique_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto projectionExpressions =
        bindProjectionExpressions(projectionBody->getProjectionExpressions());
    validateProjectionColumnsInWithClauseAreAliased(projectionExpressions);
    auto boundProjectionBody =
        bindProjectionBody(*projectionBody, rewriteProjectionInWithClause(projectionExpressions));
    validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    scope->clear();
    addExpressionsToScope(projectionExpressions);
    auto boundWithClause = std::make_unique<BoundWithClause>(std::move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

std::unique_ptr<BoundReturnClause> Binder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions =
        bindProjectionExpressions(projectionBody->getProjectionExpressions());
    auto statementResult = std::make_unique<BoundStatementResult>();
    for (auto& expression : boundProjectionExpressions) {
        statementResult->addColumn(expression);
    }
    auto boundProjectionBody = bindProjectionBody(*projectionBody, statementResult->getColumns());
    return std::make_unique<BoundReturnClause>(
        std::move(boundProjectionBody), std::move(statementResult));
}

static bool isAggregateExpression(
    const std::shared_ptr<Expression>& expression, BinderScope* scope) {
    if (expression->hasAlias() && scope->contains(expression->getAlias())) {
        return false;
    }
    if (expression->expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        return true;
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
        if (isAggregateExpression(child, scope)) {
            return true;
        }
    }
    return false;
}

static expression_vector getAggregateExpressions(
    const std::shared_ptr<Expression>& expression, BinderScope* scope) {
    expression_vector result;
    if (expression->hasAlias() && scope->contains(expression->getAlias())) {
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

std::unique_ptr<BoundProjectionBody> Binder::bindProjectionBody(
    const parser::ProjectionBody& projectionBody, const expression_vector& projectionExpressions) {
    auto boundProjectionBody = std::make_unique<BoundProjectionBody>(
        projectionBody.getIsDistinct(), projectionExpressions);
    // Bind group by & aggregate.
    expression_vector groupByExpressions;
    expression_vector aggregateExpressions;
    for (auto& expression : projectionExpressions) {
        if (isAggregateExpression(expression, scope.get())) {
            for (auto& agg : getAggregateExpressions(expression, scope.get())) {
                aggregateExpressions.push_back(agg);
            }
        } else {
            groupByExpressions.push_back(expression);
        }
    }

    if (!aggregateExpressions.empty()) {
        if (!groupByExpressions.empty()) {
            // TODO(Xiyang): we can remove augment group by. But make sure we test sufficient
            // including edge case and bug before release.
            expression_vector augmentedGroupByExpressions = groupByExpressions;
            for (auto& expression : groupByExpressions) {
                if (ExpressionUtil::isNodePattern(*expression)) {
                    auto node = (NodeExpression*)expression.get();
                    augmentedGroupByExpressions.push_back(node->getInternalID());
                } else if (ExpressionUtil::isRelPattern(*expression)) {
                    auto rel = (RelExpression*)expression.get();
                    augmentedGroupByExpressions.push_back(rel->getInternalIDProperty());
                }
            }
            boundProjectionBody->setGroupByExpressions(std::move(augmentedGroupByExpressions));
        }
        boundProjectionBody->setAggregateExpressions(std::move(aggregateExpressions));
    }
    // Bind order by
    if (projectionBody.hasOrderByExpressions()) {
        addExpressionsToScope(projectionExpressions);
        auto orderByExpressions = bindOrderByExpressions(projectionBody.getOrderByExpressions());
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        if (boundProjectionBody->hasAggregateExpressions()) {
            // TODO(Xiyang): abstract return/with clause as a temporary table and introduce
            // reference expression to solve this. Our property expression should also be changed to
            // reference expression.
            auto projectionExpressionSet =
                expression_set{projectionExpressions.begin(), projectionExpressions.end()};
            for (auto& orderByExpression : orderByExpressions) {
                if (!projectionExpressionSet.contains(orderByExpression)) {
                    throw BinderException("Order by expression " + orderByExpression->toString() +
                                          " is not in RETURN or WITH clause.");
                }
            }
        }
        boundProjectionBody->setOrderByExpressions(
            std::move(orderByExpressions), projectionBody.getSortOrders());
    }
    // Bind skip
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody->setSkipNumber(
            bindSkipLimitExpression(*projectionBody.getSkipExpression()));
    }
    // Bind limit
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody->setLimitNumber(
            bindSkipLimitExpression(*projectionBody.getLimitExpression()));
    }
    return boundProjectionBody;
}

expression_vector Binder::bindProjectionExpressions(
    const parsed_expression_vector& projectionExpressions) {
    expression_vector result;
    for (auto& expression : projectionExpressions) {
        if (expression->getExpressionType() == ExpressionType::STAR) {
            // Rewrite star expression as all expression in scope.
            if (scope->empty()) {
                throw BinderException(
                    "RETURN or WITH * is not allowed when there are no variables in scope.");
            }
            for (auto& expr : scope->getExpressions()) {
                result.push_back(expr);
            }
        } else if (expression->getExpressionType() == ExpressionType::PROPERTY) {
            auto propertyExpression = (ParsedPropertyExpression*)expression.get();
            if (propertyExpression->isStar()) {
                // Rewrite property star expression
                for (auto& expr : expressionBinder.bindPropertyStarExpression(*expression)) {
                    result.push_back(expr);
                }
            } else {
                result.push_back(expressionBinder.bindExpression(*expression));
            }
        } else {
            result.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    resolveAnyDataTypeWithDefaultType(result);
    validateProjectionColumnNamesAreUnique(result);
    return result;
}

expression_vector Binder::bindOrderByExpressions(
    const std::vector<std::unique_ptr<ParsedExpression>>& orderByExpressions) {
    expression_vector boundOrderByExpressions;
    for (auto& expression : orderByExpressions) {
        auto boundExpression = expressionBinder.bindExpression(*expression);
        if (boundExpression->dataType.getLogicalTypeID() == LogicalTypeID::NODE ||
            boundExpression->dataType.getLogicalTypeID() == LogicalTypeID::REL) {
            throw BinderException("Cannot order by " + boundExpression->toString() +
                                  ". Order by node or rel is not supported.");
        }
        boundOrderByExpressions.push_back(std::move(boundExpression));
    }
    resolveAnyDataTypeWithDefaultType(boundOrderByExpressions);
    return boundOrderByExpressions;
}

uint64_t Binder::bindSkipLimitExpression(const ParsedExpression& expression) {
    auto boundExpression = expressionBinder.bindExpression(expression);
    auto errorMsg = "The number of rows to skip/limit must be a non-negative integer.";
    if (!ExpressionVisitor::isConstant(*boundExpression)) {
        throw BinderException(errorMsg);
    }
    auto value = ((LiteralExpression&)(*boundExpression)).value.get();
    int64_t num = 0;
    // TODO: replace the following switch with value.cast()
    switch (value->getDataType()->getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        num = value->getValue<int64_t>();
    } break;
    case LogicalTypeID::INT32: {
        num = value->getValue<int32_t>();
    } break;
    case LogicalTypeID::INT16: {
        num = value->getValue<int16_t>();
    } break;
    default:
        throw BinderException(errorMsg);
    }
    if (num < 0) {
        throw BinderException(errorMsg);
    }
    return num;
}

void Binder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->toString();
        scope->addExpression(alias, expression);
    }
}

void Binder::resolveAnyDataTypeWithDefaultType(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() == LogicalTypeID::ANY) {
            ExpressionBinder::implicitCastIfNecessary(expression, LogicalTypeID::STRING);
        }
    }
}

} // namespace binder
} // namespace kuzu
