#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/query_graph/query_graph.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

/**
 * BoundMatchStatement may not have whereExpression
 */
class BoundMatchStatement {

public:
    explicit BoundMatchStatement(unique_ptr<QueryGraph> queryGraph)
        : queryGraph{move(queryGraph)} {}

    void merge(BoundMatchStatement& other) {
        queryGraph->merge(*other.queryGraph);
        if (other.whereExpression) {
            whereExpression = whereExpression ? make_shared<LogicalExpression>(AND, BOOL,
                                                    whereExpression, other.whereExpression) :
                                                other.whereExpression;
        }
    }

public:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<LogicalExpression> whereExpression;
};

} // namespace planner
} // namespace graphflow
