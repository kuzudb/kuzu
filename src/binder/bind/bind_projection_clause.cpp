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
            for (auto& property : rewriteAsAllProperties(expression, NODE)) {
                result.push_back(property);
            }
        } else if (expression->dataType.typeID == REL) {
            for (auto& property : rewriteAsAllProperties(expression, REL)) {
                result.push_back(property);
            }
        } else {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector Binder::rewriteAsAllProperties(
    const shared_ptr<Expression>& expression, DataTypeID nodeOrRelType) {
    vector<Property> properties;
    switch (nodeOrRelType) {
    case NODE: {
        auto& node = (NodeExpression&)*expression;
        for (auto tableID : node.getTableIDs()) {
            for (auto& property : catalog.getReadOnlyVersion()->getAllNodeProperties(tableID)) {
                properties.push_back(property);
            }
        }
    } break;
    case REL: {
        auto& rel = (RelExpression&)*expression;
        for (auto tableID : rel.getTableIDs()) {
            for (auto& property : catalog.getReadOnlyVersion()->getRelProperties(tableID)) {
                properties.push_back(property);
            }
        }
    } break;
    default:
        throw NotImplementedException(
            "Cannot rewrite type " + Types::dataTypeToString(nodeOrRelType));
    }
    // Make sure columns are in the same order as specified in catalog.
    vector<string> columnNames;
    unordered_set<string> existingColumnNames;
    for (auto& property : properties) {
        if (TableSchema::isReservedPropertyName(property.name)) {
            continue;
        }
        if (!existingColumnNames.contains(property.name)) {
            columnNames.push_back(property.name);
            existingColumnNames.insert(property.name);
        }
    }
    expression_vector result;
    for (auto& columnName : columnNames) {
        shared_ptr<Expression> propertyExpression;
        if (nodeOrRelType == NODE) {
            propertyExpression =
                expressionBinder.bindNodePropertyExpression(expression, columnName);
        } else {
            propertyExpression = expressionBinder.bindRelPropertyExpression(expression, columnName);
        }
        propertyExpression->setRawName(expression->getRawName() + "." + columnName);
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
