#pragma once

#include "binder/query/query_graph.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundMatchClause : public BoundReadingClause {
public:
    BoundMatchClause(QueryGraphCollection queryGraphCollection,
        common::MatchClauseType matchClauseType)
        : BoundReadingClause{common::ClauseType::MATCH},
          queryGraphCollection{std::move(queryGraphCollection)}, matchClauseType{matchClauseType} {}

    inline const QueryGraphCollection* getQueryGraphCollection() const {
        return &queryGraphCollection;
    }

    inline common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

private:
    QueryGraphCollection queryGraphCollection;
    common::MatchClauseType matchClauseType;
};

} // namespace binder
} // namespace kuzu
