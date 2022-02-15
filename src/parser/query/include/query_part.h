#pragma once

#include <cassert>

#include "src/parser/query/match_clause/include/match_clause.h"
#include "src/parser/query/return_with_clause/include/with_clause.h"

namespace graphflow {
namespace parser {

/**
 * Represents (MATCH WHERE?)* WITH WHERE?
 */
class QueryPart {

public:
    explicit QueryPart(unique_ptr<WithClause> withClause) : withClause{move(withClause)} {}

    inline void addMatchClause(unique_ptr<MatchClause> matchClause) {
        matchClauses.push_back(move(matchClause));
    }

    inline uint32_t getNumMatchClauses() const { return matchClauses.size(); }

    inline MatchClause* getMatchClause(uint32_t idx) const { return matchClauses[idx].get(); }

    inline WithClause* getWithClause() const { return withClause.get(); }

    bool operator==(const QueryPart& other) const;

    bool operator!=(const QueryPart& other) const { return !operator==(other); }

private:
    vector<unique_ptr<MatchClause>> matchClauses;
    unique_ptr<WithClause> withClause;
};

} // namespace parser
} // namespace graphflow
