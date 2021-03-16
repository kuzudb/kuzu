#pragma once

#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

class BoundMatchStatement {

public:
    explicit BoundMatchStatement(unique_ptr<QueryGraph> queryGraph)
        : queryGraph{move(queryGraph)} {}

    bool operator==(const BoundMatchStatement& other) const {
        return *queryGraph == *other.queryGraph;
    }

private:
    unique_ptr<QueryGraph> queryGraph;
};

} // namespace planner
} // namespace graphflow
