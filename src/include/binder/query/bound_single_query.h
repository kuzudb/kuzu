#pragma once

#include "binder/query/return_with_clause/bound_return_clause.h"
#include "bound_query_part.h"

namespace kuzu {
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

    inline void addReadingClause(unique_ptr<BoundReadingClause> readingClause) {
        readingClauses.push_back(move(readingClause));
    }
    inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
    inline BoundReadingClause* getReadingClause(uint32_t idx) const {
        return readingClauses[idx].get();
    }

    inline void addUpdatingClause(unique_ptr<BoundUpdatingClause> updatingClause) {
        updatingClauses.push_back(move(updatingClause));
    }
    inline uint32_t getNumUpdatingClauses() const { return updatingClauses.size(); }
    inline BoundUpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }

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
    vector<unique_ptr<BoundReadingClause>> readingClauses;
    vector<unique_ptr<BoundUpdatingClause>> updatingClauses;
    unique_ptr<BoundReturnClause> returnClause;
};

} // namespace binder
} // namespace kuzu
