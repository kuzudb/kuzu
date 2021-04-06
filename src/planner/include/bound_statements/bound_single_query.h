#pragma once

#include <utility>

#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/query_graph/query_graph.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class BoundSingleQuery {

public:
    BoundSingleQuery(unique_ptr<QueryGraph> queryGraph,
        vector<shared_ptr<LogicalExpression>> whereExprsSplitByAND)
        : queryGraph{move(queryGraph)}, whereExprsSplitByAND{move(whereExprsSplitByAND)} {}

public:
    unique_ptr<QueryGraph> queryGraph;
    vector<shared_ptr<LogicalExpression>> whereExprsSplitByAND;
};

} // namespace planner
} // namespace graphflow
