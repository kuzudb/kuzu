#pragma once

#include "query_part.h"

namespace graphflow {
namespace parser {

/**
 * Represents (QueryPart)* (MATCH WHERE?)* RETURN
 */
class SingleQuery {

public:
    explicit SingleQuery(unique_ptr<ReturnClause> returnClause)
        : returnClause{move(returnClause)} {}

    inline void addQueryPart(unique_ptr<QueryPart> queryPart) {
        queryParts.push_back(move(queryPart));
    }

    inline uint32_t getNumQueryParts() const { return queryParts.size(); }

    inline QueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline void addMatchClause(unique_ptr<MatchClause> matchClause) {
        matchClauses.push_back(move(matchClause));
    }

    inline uint32_t getNumMatchClauses() const { return matchClauses.size(); }

    inline MatchClause* getMatchClause(uint32_t idx) const { return matchClauses[idx].get(); }

    inline ReturnClause* getReturnClause() const { return returnClause.get(); }

    bool isFirstMatchOptional() const;

    bool operator==(const SingleQuery& other) const;

private:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<MatchClause>> matchClauses;
    unique_ptr<ReturnClause> returnClause;
};

} // namespace parser
} // namespace graphflow
