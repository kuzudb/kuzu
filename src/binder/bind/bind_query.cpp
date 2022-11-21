#include "binder/binder.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundRegularQuery> Binder::bindQuery(const RegularQuery& regularQuery) {
    auto boundRegularQuery = make_unique<BoundRegularQuery>(regularQuery.getIsUnionAll());
    vector<unique_ptr<BoundSingleQuery>> boundSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        // Don't clear scope within bindSingleQuery() yet because it is also used for subquery
        // binding.
        variablesInScope.clear();
        boundSingleQueries.push_back(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    validateUnionColumnsOfTheSameType(boundSingleQueries);
    for (auto& boundSingleQuery : boundSingleQueries) {
        auto normalizedSingleQuery = QueryNormalizer::normalizeQuery(*boundSingleQuery);
        validateReadNotFollowUpdate(*normalizedSingleQuery);
        validateReturnNotFollowUpdate(*normalizedSingleQuery);
        boundRegularQuery->addSingleQuery(move(normalizedSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}

unique_ptr<BoundSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    validateFirstMatchIsNotOptional(singleQuery);
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        boundSingleQuery->addQueryPart(bindQueryPart(*singleQuery.getQueryPart(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
        boundSingleQuery->addReadingClause(bindReadingClause(*singleQuery.getReadingClause(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
        boundSingleQuery->addUpdatingClause(bindUpdatingClause(*singleQuery.getUpdatingClause(i)));
    }
    if (singleQuery.hasReturnClause()) {
        boundSingleQuery->setReturnClause(
            bindReturnClause(*singleQuery.getReturnClause(), boundSingleQuery));
    }
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> Binder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < queryPart.getNumReadingClauses(); i++) {
        boundQueryPart->addReadingClause(bindReadingClause(*queryPart.getReadingClause(i)));
    }
    for (auto i = 0u; i < queryPart.getNumUpdatingClauses(); ++i) {
        boundQueryPart->addUpdatingClause(bindUpdatingClause(*queryPart.getUpdatingClause(i)));
    }
    boundQueryPart->setWithClause(bindWithClause(*queryPart.getWithClause()));
    return boundQueryPart;
}

} // namespace binder
} // namespace kuzu
