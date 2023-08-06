#pragma once

#include "binder/expression/expression.h"
#include "binder/query/query_graph.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundMatchClause : public BoundReadingClause {
public:
    explicit BoundMatchClause(std::unique_ptr<QueryGraphCollection> queryGraphCollection,
        common::MatchClauseType matchClauseType)
        : BoundReadingClause{common::ClauseType::MATCH},
          queryGraphCollection{std::move(queryGraphCollection)}, matchClauseType{matchClauseType} {}

    BoundMatchClause(const BoundMatchClause& other)
        : BoundReadingClause(common::ClauseType::MATCH),
          queryGraphCollection{other.queryGraphCollection->copy()},
          whereExpression(other.whereExpression), matchClauseType{other.matchClauseType} {}

    ~BoundMatchClause() override = default;

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }
    inline expression_vector getPredicatesSplitOnAnd() const {
        return hasWhereExpression() ? whereExpression->splitOnAND() : expression_vector{};
    }

    inline common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundMatchClause>(*this);
    }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> whereExpression;
    common::MatchClauseType matchClauseType;
};

} // namespace binder
} // namespace kuzu
