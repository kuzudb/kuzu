#include "include/bound_single_query_rewriter.h"

namespace graphflow {
namespace binder {

unique_ptr<NormalizedSingleQuery> BoundSingleQueryRewriter::rewrite(
    const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = make_unique<NormalizedSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        normalizedQuery->appendQueryPart(rewriteQueryPart(*singleQuery.getQueryPart(i)));
    }
    auto finalQueryPart = rewriteFinalQueryPart(singleQuery);
    normalizedQuery->appendQueryPart(rewriteQueryPart(*finalQueryPart));
    return normalizedQuery;
}

unique_ptr<BoundQueryPart> BoundSingleQueryRewriter::rewriteFinalQueryPart(
    const BoundSingleQuery& singleQuery) {
    auto queryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
        queryPart->addReadingClause(singleQuery.getReadingClause(i)->copy());
    }
    for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
        queryPart->addUpdatingClause(singleQuery.getUpdatingClause(i)->copy());
    }
    if (singleQuery.hasReturnClause()) {
        auto boundWithClause = make_unique<BoundWithClause>(
            singleQuery.getReturnClause()->getProjectionBody()->copy());
        queryPart->setWithClause(std::move(boundWithClause));
    }
    return queryPart;
}

unique_ptr<NormalizedQueryPart> BoundSingleQueryRewriter::rewriteQueryPart(
    const BoundQueryPart& queryPart) {
    auto normalizedQueryPart = make_unique<NormalizedQueryPart>();
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
} // namespace graphflow