#include "binder/binder.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
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

std::unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = (MatchClause&)readingClause;
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(matchClause.getPatternElements());
    auto boundMatchClause =
        make_unique<BoundMatchClause>(std::move(queryGraphCollection), matchClause.getIsOptional());
    std::shared_ptr<Expression> whereExpression;
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

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = (UnwindClause&)readingClause;
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    boundExpression =
        ExpressionBinder::implicitCastIfNecessary(boundExpression, LogicalTypeID::VAR_LIST);
    auto aliasExpression = createVariable(
        unwindClause.getAlias(), *common::VarListType::getChildType(&boundExpression->dataType));
    return make_unique<BoundUnwindClause>(std::move(boundExpression), std::move(aliasExpression));
}

} // namespace binder
} // namespace kuzu
