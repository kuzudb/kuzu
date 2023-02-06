#pragma once

#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/return_with_clause/bound_with_clause.h"
#include "binder/query/updating_clause/bound_updating_clause.h"

namespace kuzu {
namespace binder {

/**
 * Represents (Reading)* WITH
 */
class BoundQueryPart {

public:
    BoundQueryPart() = default;

    ~BoundQueryPart() = default;

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

    inline void setWithClause(std::unique_ptr<BoundWithClause> boundWithClause) {
        withClause = std::move(boundWithClause);
    }
    inline bool hasWithClause() const { return withClause != nullptr; }
    inline BoundWithClause* getWithClause() const { return withClause.get(); }

private:
    std::vector<std::unique_ptr<BoundReadingClause>> readingClauses;
    std::vector<std::unique_ptr<BoundUpdatingClause>> updatingClauses;
    std::unique_ptr<BoundWithClause> withClause;
};

} // namespace binder
} // namespace kuzu
