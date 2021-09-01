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

    BoundMatchStatement(const BoundMatchStatement& other)
        : BoundReadingStatement(MATCH_STATEMENT),
          queryGraph{make_unique<QueryGraph>(*other.queryGraph)}, whereExpression{
                                                                      other.whereExpression} {}

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    unique_ptr<BoundReadingStatement> copy() const override {
        return make_unique<BoundMatchStatement>(*this);
    }

public:
    unique_ptr<QueryGraph> queryGraph;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace graphflow
