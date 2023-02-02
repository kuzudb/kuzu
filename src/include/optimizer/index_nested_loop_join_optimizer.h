#pragma once

#include <vector>

#include "planner/logical_plan/logical_operator/base_logical_operator.h"

namespace kuzu {
namespace optimizer {

// This is a temporary optimizer that rewrites
//      Filter(a.ID=b.ID)
//      CrossProduct                   into           IndexNestedLoopJoin(b)
//   S(a)           S(b)                              S(a)
// In the absense of a generic hash join operator.
// We should merge this operator to filter push down + ASP when the generic hash join is
// implemented.
class IndexNestedLoopJoinOptimizer {
public:
    static shared_ptr<planner::LogicalOperator> rewrite(shared_ptr<planner::LogicalOperator> op);

private:
    static shared_ptr<planner::LogicalOperator> rewriteFilter(
        shared_ptr<planner::LogicalOperator> op);

    static shared_ptr<planner::LogicalOperator> rewriteCrossProduct(
        shared_ptr<planner::LogicalOperator> op, shared_ptr<Expression> predicate);

    static planner::LogicalOperator* searchScanNodeOnPipeline(planner::LogicalOperator* op);
};

} // namespace optimizer
} // namespace kuzu
