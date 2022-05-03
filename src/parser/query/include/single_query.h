#pragma once

#include "query_part.h"

namespace graphflow {
namespace parser {

class SingleQuery {

public:
    SingleQuery() = default;
    ~SingleQuery() = default;

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

    inline void addSetClause(unique_ptr<SetClause> setClause) {
        setClauses.push_back(move(setClause));
    }
    inline uint32_t getNumSetClauses() const { return setClauses.size(); }
    inline SetClause* getSetClause(uint32_t idx) const { return setClauses[idx].get(); }

    inline void setReturnClause(unique_ptr<ReturnClause> returnClause) {
        this->returnClause = move(returnClause);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline ReturnClause* getReturnClause() const { return returnClause.get(); }

    bool isFirstMatchOptional() const;

    bool operator==(const SingleQuery& other) const;

private:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<MatchClause>> matchClauses;
    vector<unique_ptr<SetClause>> setClauses;
    unique_ptr<ReturnClause> returnClause;
};

} // namespace parser
} // namespace graphflow
