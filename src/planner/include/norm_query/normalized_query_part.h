#pragma once

#include "src/binder/include/bound_statements/bound_projection_body.h"
#include "src/binder/include/query_graph/query_graph.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class NormalizedQueryPart {

public:
    NormalizedQueryPart() : lastQueryPart{false} {}

    inline bool isLastQueryPart() const { return lastQueryPart; }
    inline void setLastQueryPart(bool islastQueryPart) { lastQueryPart = islastQueryPart; }

    inline uint32_t getNumQueryGraph() const { return queryGraphs.size(); }
    inline QueryGraph* getQueryGraph(uint32_t idx) const { return queryGraphs[idx].get(); }
    inline void addQueryGraph(unique_ptr<QueryGraph> queryGraph) {
        queryGraphs.push_back(move(queryGraph));
    }

    inline shared_ptr<Expression> getQueryGraphPredicate(uint32_t idx) const {
        return queryGraphPredicates[idx];
    }
    inline void addQueryGraphPredicate(const shared_ptr<Expression>& predicate) {
        queryGraphPredicates.push_back(predicate);
    }

    inline bool isQueryGraphOptional(uint32_t idx) const { return isOptional[idx]; }
    inline void addIsQueryGraphOptional(bool isQueryGraphOptional) {
        isOptional.push_back(isQueryGraphOptional);
    }

    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }
    inline void setProjectionBody(unique_ptr<BoundProjectionBody> projectionBody) {
        this->projectionBody = move(projectionBody);
    }

    inline bool hasProjectionBodyPredicate() const { return projectionBodyPredicate != nullptr; }
    inline shared_ptr<Expression> getProjectionBodyPredicate() const {
        return projectionBodyPredicate;
    }
    inline void setProjectionBodyPredicate(const shared_ptr<Expression>& predicate) {
        projectionBodyPredicate = predicate;
    }

private:
    vector<unique_ptr<QueryGraph>> queryGraphs;
    vector<shared_ptr<Expression>> queryGraphPredicates;
    vector<bool> isOptional;

    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;

    bool lastQueryPart;
};

} // namespace planner
} // namespace graphflow
