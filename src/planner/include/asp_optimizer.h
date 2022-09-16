#pragma once

#include "src/binder/expression/include/node_expression.h"
#include "src/common/include/join_type.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class ASPOptimizer {
public:
    static bool canApplyASP(shared_ptr<NodeExpression>& joinNode, const LogicalPlan& leftPlan,
        const LogicalPlan& rightPlan);

    static void applyASP(shared_ptr<NodeExpression>& joinNode, JoinType joinType,
        LogicalPlan& leftPlan, LogicalPlan& rightPlan);

private:
    static void appendSemiMasker(shared_ptr<NodeExpression>& node, LogicalPlan& plan);
};

} // namespace planner
} // namespace graphflow
