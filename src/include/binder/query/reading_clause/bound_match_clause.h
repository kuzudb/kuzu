#pragma once

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
        : BoundReadingClause{other}, queryGraphCollection{other.queryGraphCollection->copy()},
          matchClauseType{other.matchClauseType} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundMatchClause>(*this);
    }

private:
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    common::MatchClauseType matchClauseType;
};

} // namespace binder
} // namespace kuzu
