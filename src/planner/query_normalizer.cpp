#include "src/planner/include/query_normalizer.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace planner {

unique_ptr<NormalizedQuery> QueryNormalizer::normalizeQuery(
    const BoundSingleQuery& boundSingleQuery) {
    auto normalizedQuery = make_unique<NormalizedQuery>();
    for (auto& boundQueryPart : boundSingleQuery.boundQueryParts) {
        normalizedQuery->appendQueryPart(normalizeQueryPart(*boundQueryPart, false));
    }
    auto finalQueryPart = normalizeFinalReadsAndReturnAsQueryPart(boundSingleQuery);
    normalizedQuery->appendQueryPart(normalizeQueryPart(*finalQueryPart, true));
    return normalizedQuery;
}

unique_ptr<BoundQueryPart> QueryNormalizer::normalizeFinalReadsAndReturnAsQueryPart(
    const BoundSingleQuery& boundSingleQuery) {
    auto queryPart = make_unique<BoundQueryPart>();
    for (auto& matchStatement : boundSingleQuery.boundMatchStatements) {
        queryPart->boundMatchStatements.push_back(matchStatement->copy());
    }
    queryPart->boundWithStatement =
        make_unique<BoundWithStatement>(make_unique<BoundProjectionBody>(
            *boundSingleQuery.boundReturnStatement->getBoundProjectionBody()));
    return queryPart;
}

unique_ptr<NormalizedQueryPart> QueryNormalizer::normalizeQueryPart(
    const BoundQueryPart& boundQueryPart, bool isFinalQueryPart) {
    auto normalizedQueryPart = make_unique<NormalizedQueryPart>(
        make_unique<BoundProjectionBody>(
            *boundQueryPart.boundWithStatement->getBoundProjectionBody()),
        isFinalQueryPart);
    normalizeQueryGraph(*normalizedQueryPart, boundQueryPart);
    normalizeWhereExpression(*normalizedQueryPart, boundQueryPart);
    normalizeSubqueryExpression(*normalizedQueryPart, boundQueryPart);
    return normalizedQueryPart;
}

void QueryNormalizer::normalizeQueryGraph(
    NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart) {
    for (auto& boundMatchStatement : boundQueryPart.boundMatchStatements) {
        normalizedQueryPart.addQueryGraph(*boundMatchStatement->queryGraph);
    }
}

void QueryNormalizer::normalizeWhereExpression(
    NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart) {
    for (auto& boundMatchStatement : boundQueryPart.boundMatchStatements) {
        if (boundMatchStatement->hasWhereExpression()) {
            normalizedQueryPart.addWhereExpression(boundMatchStatement->getWhereExpression());
        }
    }
    if (boundQueryPart.boundWithStatement->hasWhereExpression()) {
        normalizedQueryPart.addWhereExpression(
            boundQueryPart.boundWithStatement->getWhereExpression());
    }
}

void QueryNormalizer::normalizeSubqueryExpression(
    NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart) {
    for (auto& boundMatchStatement : boundQueryPart.boundMatchStatements) {
        if (boundMatchStatement->hasWhereExpression()) {
            for (auto& expression :
                boundMatchStatement->getWhereExpression()->getTopLevelSubSubqueryExpressions()) {
                auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
                subqueryExpression.setNormalizedSubquery(
                    normalizeQuery(*subqueryExpression.getBoundSubquery()));
            }
        }
    }
    if (boundQueryPart.boundWithStatement->hasWhereExpression()) {
        for (auto& expression : boundQueryPart.boundWithStatement->getWhereExpression()
                                    ->getTopLevelSubSubqueryExpressions()) {
            auto& subqueryExpression = (ExistentialSubqueryExpression&)*expression;
            subqueryExpression.setNormalizedSubquery(
                normalizeQuery(*subqueryExpression.getBoundSubquery()));
        }
    }
}

} // namespace planner
} // namespace graphflow
