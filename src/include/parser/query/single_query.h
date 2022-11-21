#pragma once

#include "query_part.h"

namespace kuzu {
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

    inline uint32_t getNumUpdatingClauses() const { return updatingClauses.size(); }
    inline UpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }
    inline void addUpdatingClause(unique_ptr<UpdatingClause> updatingClause) {
        updatingClauses.push_back(move(updatingClause));
    }

    inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
    inline ReadingClause* getReadingClause(uint32_t idx) const { return readingClauses[idx].get(); }
    inline void addReadingClause(unique_ptr<ReadingClause> readingClause) {
        readingClauses.push_back(move(readingClause));
    }

    inline void setReturnClause(unique_ptr<ReturnClause> returnClause) {
        this->returnClause = move(returnClause);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline ReturnClause* getReturnClause() const { return returnClause.get(); }

    bool isFirstReadingClauseOptionalMatch() const;

private:
    vector<unique_ptr<QueryPart>> queryParts;
    vector<unique_ptr<ReadingClause>> readingClauses;
    vector<unique_ptr<UpdatingClause>> updatingClauses;
    unique_ptr<ReturnClause> returnClause;
};

} // namespace parser
} // namespace kuzu
