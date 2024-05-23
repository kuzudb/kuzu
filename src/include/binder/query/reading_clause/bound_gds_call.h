#pragma once

#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/gds_function.h"

namespace kuzu {
namespace binder {

class BoundGDSCall : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::GDS_CALL;

public:
    BoundGDSCall(function::GDSFunction func, std::shared_ptr<Expression> graphExpr,
        expression_vector outExprs)
        : BoundReadingClause{clauseType_}, func{std::move(func)}, graphExpr{std::move(graphExpr)},
          outExprs{std::move(outExprs)} {}

    function::GDSFunction getFunc() const { return func; }
    std::shared_ptr<Expression> getGraphExpr() const { return graphExpr; }
    expression_vector getOutExprs() const { return outExprs; }

private:
    // Algorithm function.
    function::GDSFunction func;
    // Input graph.
    std::shared_ptr<binder::Expression> graphExpr;
    // Algorithm output expressions.
    expression_vector outExprs;
};

} // namespace binder
} // namespace kuzu
