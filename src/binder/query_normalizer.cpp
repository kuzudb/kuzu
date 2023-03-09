#include "binder/query_normalizer.h"

#include "binder/expression/existential_subquery_expression.h"

namespace kuzu {
namespace binder {

std::unique_ptr<NormalizedSingleQuery> QueryNormalizer::normalizeQuery(
    const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = std::make_unique<NormalizedSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        normalizedQuery->appendQueryPart(normalizeQueryPart(*singleQuery.getQueryPart(i)));
    }
    auto finalQueryPart = normalizeFinalMatchesAndReturnAsQueryPart(singleQuery);
    normalizedQuery->appendQueryPart(normalizeQueryPart(*finalQueryPart));
    return normalizedQuery;
}

std::unique_ptr<BoundQueryPart> QueryNormalizer::normalizeFinalMatchesAndReturnAsQueryPart(
    const BoundSingleQuery& singleQuery) {
    auto queryPart = std::make_unique<BoundQueryPart>();
    for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
        queryPart->addReadingClause(singleQuery.getReadingClause(i)->copy());
    }
    for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
        queryPart->addUpdatingClause(singleQuery.getUpdatingClause(i)->copy());
    }
    if (singleQuery.hasReturnClause()) {
        queryPart->setWithClause(
            std::make_unique<BoundWithClause>(std::make_unique<BoundProjectionBody>(
                *singleQuery.getReturnClause()->getProjectionBody())));
    }
    return queryPart;
}

std::unique_ptr<NormalizedQueryPart> QueryNormalizer::normalizeQueryPart(
    const BoundQueryPart& queryPart) {
    auto normalizedQueryPart = std::make_unique<NormalizedQueryPart>();
    for (auto i = 0u; i < queryPart.getNumReadingClauses(); i++) {
        normalizedQueryPart->addReadingClause(queryPart.getReadingClause(i)->copy());
    }
    for (auto i = 0u; i < queryPart.getNumUpdatingClauses(); ++i) {
        normalizedQueryPart->addUpdatingClause(queryPart.getUpdatingClause(i)->copy());
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
} // namespace kuzu
