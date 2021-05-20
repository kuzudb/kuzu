#pragma once

#include "src/binder/include/bound_statements/bound_reading_statement.h"
#include "src/binder/include/expression/expression.h"
#include "src/binder/include/query_graph/query_graph.h"

namespace graphflow {
namespace binder {

/**
 * BoundMatchStatement may not have whereExpression
 */
class BoundMatchStatement : public BoundReadingStatement {

public:
    explicit BoundMatchStatement(unique_ptr<QueryGraph> queryGraph)
        : BoundReadingStatement(MATCH_STATEMENT), queryGraph{move(queryGraph)} {}

    void merge(BoundMatchStatement& other) {
        queryGraph->merge(*other.queryGraph);
        if (other.whereExpression) {
            whereExpression = whereExpression ? make_shared<Expression>(AND, BOOL, whereExpression,
                                                    other.whereExpression) :
                                                other.whereExpression;
        }
    }

public:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace graphflow
