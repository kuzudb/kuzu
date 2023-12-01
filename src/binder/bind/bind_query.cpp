#include "binder/binder.h"
#include "binder/query/return_with_clause/bound_return_clause.h"
#include "binder/query/return_with_clause/bound_with_clause.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundRegularQuery> Binder::bindQuery(const RegularQuery& regularQuery) {
    std::vector<std::unique_ptr<NormalizedSingleQuery>> normalizedSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        // Don't clear scope within bindSingleQuery() yet because it is also used for subquery
        // binding.
        scope->clear();
        normalizedSingleQueries.push_back(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    validateUnionColumnsOfTheSameType(normalizedSingleQueries);
    KU_ASSERT(!normalizedSingleQueries.empty());
    auto boundRegularQuery = std::make_unique<BoundRegularQuery>(
        regularQuery.getIsUnionAll(), normalizedSingleQueries[0]->getStatementResult()->copy());
    for (auto& normalizedSingleQuery : normalizedSingleQueries) {
        validateReadNotFollowUpdate(*normalizedSingleQuery);
        boundRegularQuery->addSingleQuery(std::move(normalizedSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}

std::unique_ptr<NormalizedSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto normalizedSingleQuery = std::make_unique<NormalizedSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        normalizedSingleQuery->appendQueryPart(bindQueryPart(*singleQuery.getQueryPart(i)));
    }
    auto lastQueryPart = std::make_unique<NormalizedQueryPart>();
    for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
        lastQueryPart->addReadingClause(bindReadingClause(*singleQuery.getReadingClause(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
        lastQueryPart->addUpdatingClause(bindUpdatingClause(*singleQuery.getUpdatingClause(i)));
    }
    std::unique_ptr<BoundStatementResult> statementResult;
    if (singleQuery.hasReturnClause()) {
        auto boundReturnClause = bindReturnClause(*singleQuery.getReturnClause());
        lastQueryPart->setProjectionBody(boundReturnClause->getProjectionBody()->copy());
        statementResult = boundReturnClause->getStatementResult()->copy();
    } else {
        statementResult = BoundStatementResult::createEmptyResult();
    }
    normalizedSingleQuery->appendQueryPart(std::move(lastQueryPart));
    normalizedSingleQuery->setStatementResult(std::move(statementResult));
    return normalizedSingleQuery;
}

std::unique_ptr<NormalizedQueryPart> Binder::bindQueryPart(const QueryPart& queryPart) {
    auto normalizedQueryPart = std::make_unique<NormalizedQueryPart>();
    for (auto i = 0u; i < queryPart.getNumReadingClauses(); i++) {
        normalizedQueryPart->addReadingClause(bindReadingClause(*queryPart.getReadingClause(i)));
    }
    for (auto i = 0u; i < queryPart.getNumUpdatingClauses(); ++i) {
        normalizedQueryPart->addUpdatingClause(bindUpdatingClause(*queryPart.getUpdatingClause(i)));
    }
    auto boundWithClause = bindWithClause(*queryPart.getWithClause());
    normalizedQueryPart->setProjectionBody(boundWithClause->getProjectionBody()->copy());
    if (boundWithClause->hasWhereExpression()) {
        normalizedQueryPart->setProjectionBodyPredicate(boundWithClause->getWhereExpression());
    }
    return normalizedQueryPart;
}

} // namespace binder
} // namespace kuzu
