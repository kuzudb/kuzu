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

    inline void addQueryPart(std::unique_ptr<BoundQueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline BoundQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline void addReadingClause(std::unique_ptr<BoundReadingClause> readingClause) {
        readingClauses.push_back(std::move(readingClause));
    }
    inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
    inline BoundReadingClause* getReadingClause(uint32_t idx) const {
        return readingClauses[idx].get();
    }

    inline void addUpdatingClause(std::unique_ptr<BoundUpdatingClause> updatingClause) {
        updatingClauses.push_back(std::move(updatingClause));
    }
    inline uint32_t getNumUpdatingClauses() const { return updatingClauses.size(); }
    inline BoundUpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }

    inline void setReturnClause(std::unique_ptr<BoundReturnClause> boundReturnClause) {
        returnClause = std::move(boundReturnClause);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline BoundReturnClause* getReturnClause() const { return returnClause.get(); }

    inline expression_vector getExpressionsToCollect() const {
        return hasReturnClause() ? returnClause->getStatementResult()->getColumns() :
                                   expression_vector{};
    }

private:
    std::vector<std::unique_ptr<BoundQueryPart>> queryParts;
    std::vector<std::unique_ptr<BoundReadingClause>> readingClauses;
    std::vector<std::unique_ptr<BoundUpdatingClause>> updatingClauses;
    std::unique_ptr<BoundReturnClause> returnClause;
};

} // namespace binder
} // namespace kuzu
