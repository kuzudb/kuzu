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

    void addQueryGraph(unique_ptr<QueryGraph> queryGraph, shared_ptr<Expression> predicate,
        bool isQueryGraphOptional);
    inline uint32_t getNumQueryGraph() const { return queryGraphs.size(); }
    inline QueryGraph* getQueryGraph(uint32_t idx) const { return queryGraphs[idx].get(); }

    inline bool hasQueryGraphPredicate(uint32_t idx) const {
        return queryGraphPredicates[idx] != nullptr;
    }
    inline shared_ptr<Expression> getQueryGraphPredicate(uint32_t idx) const {
        return queryGraphPredicates[idx];
    }

    inline bool isQueryGraphOptional(uint32_t idx) const { return isOptional[idx]; }

    inline void addUnwindClauses(unique_ptr<BoundUnwindClause> boundUnwindClause) {
        unwindClauses.push_back(move(boundUnwindClause));
    }
    inline bool hasUnwindClause() const { return !unwindClauses.empty(); }
    inline uint32_t getNumUnwindClause() const { return unwindClauses.size(); }
    inline BoundUnwindClause* getUnwindClause(uint32_t idx) const {
        return unwindClauses[idx].get();
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
    // read
    vector<unique_ptr<QueryGraph>> queryGraphs;
    expression_vector queryGraphPredicates;
    vector<bool> isOptional;
    // TODO (Anurag): Add BoundReadingClause here for holding match, unwind etc. clauses
    vector<unique_ptr<BoundUnwindClause>> unwindClauses;

    vector<unique_ptr<BoundUpdatingClause>> updatingClauses;

    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;
};

} // namespace binder
} // namespace graphflow
