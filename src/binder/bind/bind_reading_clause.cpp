#include "binder/binder.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        return bindMatchClause((MatchClause&)readingClause);
    }
    case ClauseType::UNWIND: {
        return bindUnwindClause((UnwindClause&)readingClause);
    }
    default:
        throw NotImplementedException("bindReadingClause().");
    }
}

unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = (MatchClause&)readingClause;
    auto prevVariablesInScope = variablesInScope;
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(matchClause.getPatternElements());
    auto boundMatchClause =
        make_unique<BoundMatchClause>(std::move(queryGraphCollection), matchClause.getIsOptional());
    shared_ptr<Expression> whereExpression;
    if (matchClause.hasWhereClause()) {
        whereExpression = bindWhereExpression(*matchClause.getWhereClause());
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto& keyValPairs : propertyCollection->getAllPropertyKeyValPairs()) {
        auto predicate = expressionBinder.bindComparisonExpression(
            EQUALS, expression_vector{keyValPairs.first, keyValPairs.second});
        if (whereExpression != nullptr) {
            whereExpression = expressionBinder.bindBooleanExpression(
                AND, expression_vector{whereExpression, predicate});
        } else {
            whereExpression = predicate;
        }
    }
    boundMatchClause->setWhereExpression(std::move(whereExpression));
    return boundMatchClause;
}

unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = (UnwindClause&)readingClause;
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    boundExpression = ExpressionBinder::implicitCastIfNecessary(boundExpression, LIST);
    auto aliasExpression =
        createVariable(unwindClause.getAlias(), *boundExpression->dataType.childType);
    return make_unique<BoundUnwindClause>(move(boundExpression), move(aliasExpression));
}

} // namespace binder
} // namespace kuzu
