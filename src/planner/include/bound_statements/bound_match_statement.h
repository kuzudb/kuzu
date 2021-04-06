#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/query_graph/query_graph.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class BoundMatchStatement {

public:
    explicit BoundMatchStatement(unique_ptr<QueryGraph> queryGraph)
        : queryGraph{move(queryGraph)} {}

    BoundMatchStatement(
        unique_ptr<QueryGraph> queryGraph, unique_ptr<LogicalExpression> whereExpression)
        : queryGraph{move(queryGraph)}, whereExpression{move(whereExpression)} {}

    QueryGraph& getQueryGraph() { return *queryGraph; }

    bool operator==(const BoundMatchStatement& other) const {
        return *queryGraph == *other.queryGraph;
    }

public:
    unique_ptr<QueryGraph> queryGraph;
    unique_ptr<LogicalExpression> whereExpression;
};

} // namespace planner
} // namespace graphflow
