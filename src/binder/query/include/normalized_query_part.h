#pragma once

#include "src/binder/query/match_clause/include/query_graph.h"
#include "src/binder/query/return_with_clause/include/bound_projection_body.h"

namespace graphflow {
namespace binder {

class NormalizedQueryPart {

public:
    NormalizedQueryPart(unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

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

    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    inline bool hasProjectionBodyPredicate() const { return projectionBodyPredicate != nullptr; }

    inline shared_ptr<Expression> getProjectionBodyPredicate() const {
        return projectionBodyPredicate;
    }

    inline void setProjectionBodyPredicate(const shared_ptr<Expression>& predicate) {
        projectionBodyPredicate = predicate;
    }

    expression_vector getAllPropertyExpressions() const;

private:
    vector<unique_ptr<QueryGraph>> queryGraphs;
    expression_vector queryGraphPredicates;
    vector<bool> isOptional;

    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;
};

} // namespace binder
} // namespace graphflow
