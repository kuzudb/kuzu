#pragma once

#include "src/binder/query/match_clause/include/bound_match_clause.h"
#include "src/binder/query/return_with_clause/include/bound_with_clause.h"
#include "src/binder/query/set_clause/include/bound_set_clause.h"

namespace graphflow {
namespace binder {

/**
 * Represents (Reading)* WITH
 */
class BoundQueryPart {

public:
    BoundQueryPart() = default;

    ~BoundQueryPart() = default;

    inline void addMatchClause(unique_ptr<BoundMatchClause> matchClause) {
        matchClauses.push_back(move(matchClause));
    }
    inline uint32_t getNumMatchClauses() const { return matchClauses.size(); }
    inline BoundMatchClause* getMatchClause(uint32_t idx) const { return matchClauses[idx].get(); }

    inline void addSetClause(unique_ptr<BoundSetClause> setClause) {
        setClauses.push_back(move(setClause));
    }
    inline uint32_t getNumSetClauses() const { return setClauses.size(); }
    inline BoundSetClause* getSetClause(uint32_t idx) const { return setClauses[idx].get(); }

    inline void setWithClause(unique_ptr<BoundWithClause> boundWithClause) {
        withClause = move(boundWithClause);
    }
    inline bool hasWithClause() const { return withClause != nullptr; }
    inline BoundWithClause* getWithClause() const { return withClause.get(); }

private:
    vector<unique_ptr<BoundMatchClause>> matchClauses;
    vector<unique_ptr<BoundSetClause>> setClauses;
    unique_ptr<BoundWithClause> withClause;
};

} // namespace binder
} // namespace graphflow
