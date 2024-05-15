#pragma once

#include "binder/query/query_graph.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundMatchClause : public BoundReadingClause {
public:
    BoundMatchClause(QueryGraphCollection collection, common::MatchClauseType matchClauseType)
        : BoundReadingClause{common::ClauseType::MATCH}, collection{std::move(collection)},
          matchClauseType{matchClauseType} {}

    QueryGraphCollection* getQueryGraphCollectionUnsafe() { return &collection; }
    const QueryGraphCollection* getQueryGraphCollection() const { return &collection; }

    common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

private:
    QueryGraphCollection collection;
    common::MatchClauseType matchClauseType;
};

} // namespace binder
} // namespace kuzu
