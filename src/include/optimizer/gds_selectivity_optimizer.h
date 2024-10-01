#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace optimizer {


// Remove hash join
class GDSSelectivityOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

    std::shared_ptr<planner::LogicalOperator> visitOperator(
        const std::shared_ptr<planner::LogicalOperator>& op);

private:
    std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
        std::shared_ptr<planner::LogicalOperator> op);
};
} // namespace optimizer
} // namespace kuzu
