#include "binder/binder.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundRegularQuery> Binder::bindQuery(const RegularQuery& regularQuery) {
    std::vector<std::unique_ptr<BoundSingleQuery>> boundSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        // Don't clear scope within bindSingleQuery() yet because it is also used for subquery
        // binding.
        variableScope->clear();
        boundSingleQueries.push_back(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    validateUnionColumnsOfTheSameType(boundSingleQueries);
    assert(!boundSingleQueries.empty());
    auto statementResult =
        boundSingleQueries[0]->hasReturnClause() ?
            boundSingleQueries[0]->getReturnClause()->getStatementResult()->copy() :
            BoundStatementResult::createEmptyResult();
    auto boundRegularQuery = std::make_unique<BoundRegularQuery>(
        regularQuery.getIsUnionAll(), std::move(statementResult));
    for (auto& boundSingleQuery : boundSingleQueries) {
        auto normalizedSingleQuery = QueryNormalizer::normalizeQuery(*boundSingleQuery);
        validateReadNotFollowUpdate(*normalizedSingleQuery);
        validateReturnNotFollowUpdate(*normalizedSingleQuery);
        boundRegularQuery->addSingleQuery(std::move(normalizedSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}

std::unique_ptr<BoundSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    validateFirstMatchIsNotOptional(singleQuery);
    auto boundSingleQuery = std::make_unique<BoundSingleQuery>();
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
        boundSingleQuery->setReturnClause(bindReturnClause(*singleQuery.getReturnClause()));
    }
    return boundSingleQuery;
}

std::unique_ptr<BoundQueryPart> Binder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = std::make_unique<BoundQueryPart>();
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
