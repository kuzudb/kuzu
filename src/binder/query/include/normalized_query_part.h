#pragma once

#include "src/binder/query/match_clause/include/query_graph.h"
#include "src/binder/query/return_with_clause/include/bound_projection_body.h"
#include "src/binder/query/set_clause/include/bound_set_clause.h"

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

    inline void addSetClause(unique_ptr<BoundSetClause> boundSetClause) {
        setClauses.push_back(move(boundSetClause));
    }
    inline bool hasSetClause() const { return !setClauses.empty(); }
    inline uint32_t getNumSetClause() const { return setClauses.size(); }
    inline BoundSetClause* getSetClause(uint32_t idx) const { return setClauses[idx].get(); }

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

    // update
    vector<unique_ptr<BoundSetClause>> setClauses;

    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;
};

} // namespace binder
} // namespace graphflow
