#include "src/planner/include/query_normalizer.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace planner {

unique_ptr<NormalizedQuery> QueryNormalizer::normalizeQuery(
    const BoundSingleQuery& boundSingleQuery) {
    auto normalizedQuery = make_unique<NormalizedQuery>();
    for (auto& boundQueryPart : boundSingleQuery.boundQueryParts) {
        normalizedQuery->appendQueryPart(normalizeQueryPart(*boundQueryPart));
    }
    auto finalQueryPart = normalizeFinalReadsAndReturnAsQueryPart(boundSingleQuery);
    normalizedQuery->appendQueryPart(normalizeQueryPart(*finalQueryPart));
    normalizedQuery->markLastQueryPart();
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
    const BoundQueryPart& boundQueryPart) {
    normalizeSubqueryExpression(boundQueryPart);
    auto normalizedQueryPart = make_unique<NormalizedQueryPart>();
    for (auto& matchStatement : boundQueryPart.boundMatchStatements) {
        normalizedQueryPart->addQueryGraph(matchStatement->queryGraph->copy());
        normalizedQueryPart->addQueryGraphPredicate(
            matchStatement->hasWhereExpression() ? matchStatement->getWhereExpression() : nullptr);
        normalizedQueryPart->addIsQueryGraphOptional(matchStatement->isOptional);
    }
    normalizedQueryPart->setProjectionBody(
        boundQueryPart.boundWithStatement->getBoundProjectionBody()->copy());
    if (boundQueryPart.boundWithStatement->hasWhereExpression()) {
        normalizedQueryPart->setProjectionBodyPredicate(
            boundQueryPart.boundWithStatement->getWhereExpression());
    }
    return normalizedQueryPart;
}

void QueryNormalizer::normalizeSubqueryExpression(const BoundQueryPart& boundQueryPart) {
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
