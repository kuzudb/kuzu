#include "binder/binder.h"
#include "binder/expression/literal_expression.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->getContainsStar());
    validateProjectionColumnsInWithClauseAreAliased(boundProjectionExpressions);
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    variablesInScope.clear();
    addExpressionsToScope(boundProjectionBody->getProjectionExpressions());
    auto boundWithClause = make_unique<BoundWithClause>(move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

unique_ptr<BoundReturnClause> Binder::bindReturnClause(
    const ReturnClause& returnClause, unique_ptr<BoundSingleQuery>& boundSingleQuery) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions = rewriteProjectionExpressions(bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->getContainsStar()));
    validateProjectionColumnHasNoInternalType(boundProjectionExpressions);
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    return make_unique<BoundReturnClause>(move(boundProjectionBody));
}

expression_vector Binder::bindProjectionExpressions(
    const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar) {
    expression_vector boundProjectionExpressions;
    for (auto& expression : projectionExpressions) {
        boundProjectionExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    if (containsStar) {
        if (variablesInScope.empty()) {
            throw BinderException(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            boundProjectionExpressions.push_back(expression);
        }
    }
    resolveAnyDataTypeWithDefaultType(boundProjectionExpressions);
    validateProjectionColumnNamesAreUnique(boundProjectionExpressions);
    return boundProjectionExpressions;
}

expression_vector Binder::rewriteProjectionExpressions(const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == NODE) {
            for (auto& property : rewriteNodeAsAllProperties(expression)) {
                result.push_back(property);
            }
        } else if (expression->dataType.typeID == REL) {
            for (auto& property : rewriteRelAsAllProperties(expression)) {
                result.push_back(property);
            }
        } else {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector Binder::rewriteNodeAsAllProperties(const shared_ptr<Expression>& expression) {
    auto& node = (NodeExpression&)*expression;
    auto numColumns = 0u;
    // Make sure columns are in the same order as specified in catalog.
    unordered_map<uint32_t, string> colIdxToName;
    unordered_map<string, vector<Property>> nameToProperties;
    for (auto tableID : node.getTableIDs()) {
        for (auto& property : catalog.getReadOnlyVersion()->getAllNodeProperties(tableID)) {
            if (!nameToProperties.contains(property.name)) {
                nameToProperties.insert({property.name, vector<Property>{}});
                colIdxToName.insert({numColumns, property.name});
                numColumns++;
            }
            nameToProperties.at(property.name).push_back(property);
        }
    }
    expression_vector result;
    for (auto i = 0u; i < numColumns; ++i) {
        auto name = colIdxToName.at(i);
        auto properties = nameToProperties.at(name);
        auto propertyExpression =
            expressionBinder.bindNodePropertyExpression(expression, properties);
        propertyExpression->setRawName(expression->getRawName() + "." + name);
        result.emplace_back(propertyExpression);
    }
    return result;
}

expression_vector Binder::rewriteRelAsAllProperties(const shared_ptr<Expression>& expression) {
    auto& rel = (RelExpression&)*expression;
    expression_vector result;
    for (auto& property : catalog.getReadOnlyVersion()->getRelProperties(rel.getTableID())) {
        if (TableSchema::isReservedPropertyName(property.name)) {
            continue;
        }
        auto propertyExpression = expressionBinder.bindRelPropertyExpression(expression, property);
        propertyExpression->setRawName(expression->getRawName() + "." + property.name);
        result.emplace_back(propertyExpression);
    }
    return result;
}

void Binder::bindOrderBySkipLimitIfNecessary(
    BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody) {
    if (projectionBody.hasOrderByExpressions()) {
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        if (boundProjectionBody.hasAggregationExpressions()) {
            variablesInScope.clear();
        }
        addExpressionsToScope(boundProjectionBody.getProjectionExpressions());
        boundProjectionBody.setOrderByExpressions(
            bindOrderByExpressions(projectionBody.getOrderByExpressions()),
            projectionBody.getSortOrders());
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
    const vector<unique_ptr<ParsedExpression>>& orderByExpressions) {
    expression_vector boundOrderByExpressions;
    for (auto& expression : orderByExpressions) {
        auto boundExpression = expressionBinder.bindExpression(*expression);
        if (boundExpression->dataType.typeID == NODE || boundExpression->dataType.typeID == REL) {
            throw BinderException("Cannot order by " + boundExpression->getRawName() +
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
        ((LiteralExpression&)(*boundExpression)).getDataType().typeID != INT64) {
        throw BinderException("The number of rows to skip/limit must be a non-negative integer.");
    }
    return ((LiteralExpression&)(*boundExpression)).literal->val.int64Val;
}

void Binder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->getRawName();
        variablesInScope.insert({alias, expression});
    }
}

void Binder::resolveAnyDataTypeWithDefaultType(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == ANY) {
            ExpressionBinder::implicitCastIfNecessary(expression, STRING);
        }
    }
}

} // namespace binder
} // namespace kuzu
