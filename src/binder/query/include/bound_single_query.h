#pragma once

#include "bound_query_part.h"

#include "src/binder/query/return_with_clause/include/bound_return_clause.h"

namespace graphflow {
namespace binder {

/**
 * Represents (QueryPart)* (Reading)* RETURN
 */
class BoundSingleQuery {

public:
    BoundSingleQuery() = default;
    ~BoundSingleQuery() = default;

    inline void addQueryPart(unique_ptr<BoundQueryPart> queryPart) {
        queryParts.push_back(move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline BoundQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

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

    inline void setReturnClause(unique_ptr<BoundReturnClause> boundReturnClause) {
        returnClause = move(boundReturnClause);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline BoundReturnClause* getReturnClause() const { return returnClause.get(); }

    inline expression_vector getExpressionsToReturn() const {
        return returnClause->getProjectionBody()->getProjectionExpressions();
    }

private:
    vector<unique_ptr<BoundQueryPart>> queryParts;
    vector<unique_ptr<BoundMatchClause>> matchClauses;
    vector<unique_ptr<BoundSetClause>> setClauses;
    unique_ptr<BoundReturnClause> returnClause;
};

} // namespace binder
} // namespace graphflow
