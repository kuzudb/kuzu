#include "include/bound_single_query_rewriter.h"

#include "src/binder/expression/include/function_expression.h"
#include "src/binder/query/reading_clause/include/bound_match_clause.h"
#include "src/function/boolean/include/vector_boolean_operations.h"

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
    if (queryPart.getNumReadingClauses() != 0) {
        normalizedQueryPart->addReadingClause(queryPart.getReadingClause(0)->copy());
        auto former = normalizedQueryPart->getReadingClause(0);
        for (auto i = 1u; i < queryPart.getNumReadingClauses(); i++) {
            auto latter = queryPart.getReadingClause(i);
            if (canMergeReadingClause(*former, *latter)) {
                mergeReadingClause(*former, *latter);
            } else {
                normalizedQueryPart->addReadingClause(latter->copy());
            }
            former = normalizedQueryPart->getLastReadingClause();
        }
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

bool BoundSingleQueryRewriter::canMergeReadingClause(
    const BoundReadingClause& former, const BoundReadingClause& latter) {
    if (former.getClauseType() != ClauseType::MATCH ||
        latter.getClauseType() != ClauseType::MATCH) {
        return false;
    }
    auto& formerMatch = (BoundMatchClause&)former;
    auto& latterMatch = (BoundMatchClause&)latter;
    if (formerMatch.getIsOptional() || latterMatch.getIsOptional()) {
        return false;
    }
    return true;
}

void BoundSingleQueryRewriter::mergeReadingClause(
    BoundReadingClause& former, const BoundReadingClause& latter) {
    auto& formerMatch = (BoundMatchClause&)former;
    auto& latterMatch = (BoundMatchClause&)latter;
    formerMatch.getQueryGraphCollection()->merge(*latterMatch.getQueryGraphCollection());
    if (!latterMatch.hasWhereExpression()) {
        return;
    }
    if (formerMatch.hasWhereExpression()) {
        auto children =
            expression_vector{formerMatch.getWhereExpression(), latterMatch.getWhereExpression()};
        auto execFunc = VectorBooleanOperations::bindExecFunction(AND, children);
        auto selectFunc = VectorBooleanOperations::bindSelectFunction(AND, children);
        auto uniqueExpressionName =
            ScalarFunctionExpression::getUniqueName(expressionTypeToString(AND), children);
        formerMatch.setWhereExpression(make_shared<ScalarFunctionExpression>(AND, DataType(BOOL),
            std::move(children), std::move(execFunc), std::move(selectFunc), uniqueExpressionName));
    } else {
        formerMatch.setWhereExpression(latterMatch.getWhereExpression());
    }
}

} // namespace binder
} // namespace graphflow