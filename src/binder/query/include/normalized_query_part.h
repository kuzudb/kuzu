#pragma once

#include "src/binder/query/reading_clause/include/bound_unwind_clause.h"
#include "src/binder/query/reading_clause/include/query_graph.h"
#include "src/binder/query/return_with_clause/include/bound_projection_body.h"
#include "src/binder/query/updating_clause/include/bound_updating_clause.h"

namespace graphflow {
namespace binder {

class NormalizedQueryPart {

public:
    NormalizedQueryPart() = default;
    ~NormalizedQueryPart() = default;

    inline void addReadingClause(unique_ptr<BoundReadingClause> boundReadingClause) {
        readingClauses.push_back(move(boundReadingClause));
    }
    inline bool hasReadingClause() const { return !readingClauses.empty(); }
    inline uint32_t getNumReadingClause() const { return readingClauses.size(); }
    inline BoundReadingClause* getReadingClause(uint32_t idx) const {
        return readingClauses[idx].get();
    }

    inline void addUpdatingClause(unique_ptr<BoundUpdatingClause> boundUpdatingClause) {
        updatingClauses.push_back(move(boundUpdatingClause));
    }
    inline bool hasUpdatingClause() const { return !updatingClauses.empty(); }
    inline uint32_t getNumUpdatingClause() const { return updatingClauses.size(); }
    inline BoundUpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }

    inline void setProjectionBody(unique_ptr<BoundProjectionBody> boundProjectionBody) {
        projectionBody = move(boundProjectionBody);
    }
    inline bool hasProjectionBody() const { return projectionBody != nullptr; }
    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    inline bool hasProjectionBodyPredicate() const { return projectionBodyPredicate != nullptr; }
    inline shared_ptr<Expression> getProjectionBodyPredicate() const {
        return projectionBodyPredicate;
    }
    inline void setProjectionBodyPredicate(const shared_ptr<Expression>& predicate) {
        projectionBodyPredicate = predicate;
    }

    expression_vector getPropertiesToRead() const;

private:
    vector<unique_ptr<BoundReadingClause>> readingClauses;

    vector<unique_ptr<BoundUpdatingClause>> updatingClauses;

    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;
};

} // namespace binder
} // namespace graphflow
