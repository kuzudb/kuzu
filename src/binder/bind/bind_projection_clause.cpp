#include "binder/binder.h"
#include "binder/expression/literal_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->containsStar());
    validateProjectionColumnsInWithClauseAreAliased(boundProjectionExpressions);
    auto boundProjectionBody = std::make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), std::move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    variableScope->clear();
    addExpressionsToScope(boundProjectionBody->getProjectionExpressions());
    auto boundWithClause = std::make_unique<BoundWithClause>(std::move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

std::unique_ptr<BoundReturnClause> Binder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->containsStar());
    auto statementResult = std::make_unique<BoundStatementResult>();
    for (auto& expression : boundProjectionExpressions) {
        auto dataType = expression->getDataType();
        if (dataType.getLogicalTypeID() == common::LogicalTypeID::NODE ||
            dataType.getLogicalTypeID() == common::LogicalTypeID::REL) {
            statementResult->addColumn(expression, rewriteNodeOrRelExpression(*expression));
        } else {
            statementResult->addColumn(expression, expression_vector{expression});
        }
    }
    auto boundProjectionBody = std::make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), statementResult->getExpressionsToCollect());
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    return std::make_unique<BoundReturnClause>(
        std::move(boundProjectionBody), std::move(statementResult));
}

expression_vector Binder::bindProjectionExpressions(
    const std::vector<std::unique_ptr<ParsedExpression>>& projectionExpressions,
    bool containsStar) {
    expression_vector boundProjectionExpressions;
    for (auto& expression : projectionExpressions) {
        boundProjectionExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    if (containsStar) {
        if (variableScope->empty()) {
            throw BinderException(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& expression : variableScope->getExpressions()) {
            boundProjectionExpressions.push_back(expression);
        }
    }
    resolveAnyDataTypeWithDefaultType(boundProjectionExpressions);
    validateProjectionColumnNamesAreUnique(boundProjectionExpressions);
    return boundProjectionExpressions;
}

expression_vector Binder::rewriteNodeOrRelExpression(const Expression& expression) {
    if (expression.dataType.getLogicalTypeID() == common::LogicalTypeID::NODE) {
        return rewriteNodeExpression(expression);
    } else {
        assert(expression.dataType.getLogicalTypeID() == common::LogicalTypeID::REL);
        return rewriteRelExpression(expression);
    }
}

expression_vector Binder::rewriteNodeExpression(const kuzu::binder::Expression& expression) {
    expression_vector result;
    auto& node = (NodeExpression&)expression;
    result.push_back(node.getInternalIDProperty());
    result.push_back(expressionBinder.bindLabelFunction(node));
    for (auto& property : node.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    return result;
}

expression_vector Binder::rewriteRelExpression(const Expression& expression) {
    expression_vector result;
    auto& rel = (RelExpression&)expression;
    result.push_back(rel.getSrcNode()->getInternalIDProperty());
    result.push_back(rel.getDstNode()->getInternalIDProperty());
    result.push_back(expressionBinder.bindLabelFunction(rel));
    for (auto& property : rel.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    return result;
}

void Binder::bindOrderBySkipLimitIfNecessary(
    BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody) {
    auto projectionExpressions = boundProjectionBody.getProjectionExpressions();
    if (projectionBody.hasOrderByExpressions()) {
        addExpressionsToScope(projectionExpressions);
        auto orderByExpressions = bindOrderByExpressions(projectionBody.getOrderByExpressions());
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        if (boundProjectionBody.hasAggregationExpressions()) {
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
        boundProjectionBody.setOrderByExpressions(
            std::move(orderByExpressions), projectionBody.getSortOrders());
    }
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody.setSkipNumber(
            bindSkipLimitExpression(*projectionBody.getSkipExpression()));
    }
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody.setLimitNumber(
            bindSkipLimitExpression(*projectionBody.getLimitExpression()));
    }
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
    // We currently do not support the number of rows to skip/limit written as an expression (eg.
    // SKIP 3 + 2 is not supported).
    if (expression.getExpressionType() != LITERAL ||
        ((LiteralExpression&)(*boundExpression)).getDataType().getLogicalTypeID() !=
            LogicalTypeID::INT64) {
        throw BinderException("The number of rows to skip/limit must be a non-negative integer.");
    }
    return ((LiteralExpression&)(*boundExpression)).value->getValue<int64_t>();
}

void Binder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->toString();
        variableScope->addExpression(alias, expression);
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
