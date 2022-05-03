#include "include/query_normalizer.h"

#include "src/binder/expression/include/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

unique_ptr<NormalizedSingleQuery> QueryNormalizer::normalizeQuery(
    const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = make_unique<NormalizedSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        normalizedQuery->appendQueryPart(normalizeQueryPart(*singleQuery.getQueryPart(i)));
    }
    auto finalQueryPart = normalizeFinalMatchesAndReturnAsQueryPart(singleQuery);
    normalizedQuery->appendQueryPart(normalizeQueryPart(*finalQueryPart));
    return normalizedQuery;
}

unique_ptr<BoundQueryPart> QueryNormalizer::normalizeFinalMatchesAndReturnAsQueryPart(
    const BoundSingleQuery& singleQuery) {
    auto queryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < singleQuery.getNumMatchClauses(); ++i) {
        queryPart->addMatchClause(singleQuery.getMatchClause(i)->copy());
    }
    for (auto i = 0u; i < singleQuery.getNumSetClauses(); ++i) {
        auto setClause = singleQuery.getSetClause(i);
        queryPart->addSetClause(make_unique<BoundSetClause>(*setClause));
    }
    if (singleQuery.hasReturnClause()) {
        queryPart->setWithClause(make_unique<BoundWithClause>(
            make_unique<BoundProjectionBody>(*singleQuery.getReturnClause()->getProjectionBody())));
    }
    return queryPart;
}

unique_ptr<NormalizedQueryPart> QueryNormalizer::normalizeQueryPart(
    const BoundQueryPart& queryPart) {
    auto normalizedQueryPart = make_unique<NormalizedQueryPart>();
    for (auto i = 0u; i < queryPart.getNumMatchClauses(); ++i) {
        auto matchClause = queryPart.getMatchClause(i);
        normalizedQueryPart->addQueryGraph(matchClause->getQueryGraph()->copy(),
            matchClause->hasWhereExpression() ? matchClause->getWhereExpression() : nullptr,
            matchClause->getIsOptional());
    }
    for (auto i = 0u; i < queryPart.getNumSetClauses(); ++i) {
        auto setClause = queryPart.getSetClause(i);
        normalizedQueryPart->addSetClause(make_unique<BoundSetClause>(*setClause));
    }
    if (queryPart.hasWithClause()) {
        auto withClause = queryPart.getWithClause();
        normalizedQueryPart->setProjectionBody(withClause->getProjectionBody()->copy());
        if (withClause->hasWhereExpression()) {
            normalizedQueryPart->setProjectionBodyPredicate(withClause->getWhereExpression());
        }
    }
    return normalizedQueryPart;
}

} // namespace binder
} // namespace graphflow
