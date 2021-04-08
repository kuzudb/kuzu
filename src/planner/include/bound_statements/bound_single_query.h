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
        vector<shared_ptr<LogicalExpression>> whereExpressionsSplitOnAND)
        : queryGraph{move(queryGraph)}, whereExpressionsSplitOnAND{whereExpressionsSplitOnAND} {}

public:
    unique_ptr<QueryGraph> queryGraph;
    vector<shared_ptr<LogicalExpression>> whereExpressionsSplitOnAND;
};

} // namespace planner
} // namespace graphflow
