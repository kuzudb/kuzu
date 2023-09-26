#pragma once

#include "binder/expression/expression.h"
#include "binder/query/query_graph.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundMatchClause : public BoundReadingClause {
public:
    BoundMatchClause(std::unique_ptr<QueryGraphCollection> queryGraphCollection,
        common::MatchClauseType matchClauseType)
        : BoundReadingClause{common::ClauseType::MATCH},
          queryGraphCollection{std::move(queryGraphCollection)}, matchClauseType{matchClauseType} {}
    BoundMatchClause(const BoundMatchClause& other)
        : BoundReadingClause(common::ClauseType::MATCH),
          queryGraphCollection{other.queryGraphCollection->copy()},
          wherePredicate(other.wherePredicate), matchClauseType{other.matchClauseType} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWherePredicate(std::shared_ptr<Expression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline std::shared_ptr<Expression> getWherePredicate() const { return wherePredicate; }
    inline expression_vector getPredicatesSplitOnAnd() const {
        return hasWherePredicate() ? wherePredicate->splitOnAND() : expression_vector{};
    }

    inline common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundMatchClause>(*this);
    }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> wherePredicate;
    common::MatchClauseType matchClauseType;
};

} // namespace binder
} // namespace kuzu
