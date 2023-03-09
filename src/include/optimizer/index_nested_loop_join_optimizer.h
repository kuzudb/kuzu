#pragma once

#include "logical_operator_visitor.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

// This is a temporary optimizer that rewrites
//      Filter(a.ID=b.ID)
//      CrossProduct                   into           IndexNestedLoopJoin(b)
//   S(a)           S(b)                              S(a)
// In the absense of a generic hash join operator.
// We should merge this operator to filter push down + ASP when the generic hash join is
// implemented.
class IndexNestedLoopJoinOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> visitOperator(
        std::shared_ptr<planner::LogicalOperator> op);

    std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
        std::shared_ptr<planner::LogicalOperator> op) override;

    std::shared_ptr<planner::LogicalOperator> rewriteCrossProduct(
        std::shared_ptr<planner::LogicalOperator> op,
        std::shared_ptr<binder::Expression> predicate);

    planner::LogicalOperator* searchScanNodeOnPipeline(planner::LogicalOperator* op);
};

} // namespace optimizer
} // namespace kuzu
