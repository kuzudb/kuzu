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

    inline void setWithClause(unique_ptr<BoundWithClause> boundWithClause) {
        withClause = move(boundWithClause);
    }
    inline bool hasWithClause() const { return withClause != nullptr; }
    inline BoundWithClause* getWithClause() const { return withClause.get(); }

private:
    vector<unique_ptr<BoundReadingClause>> readingClauses;
    vector<unique_ptr<BoundUpdatingClause>> updatingClauses;
    unique_ptr<BoundWithClause> withClause;
};

} // namespace binder
} // namespace kuzu
