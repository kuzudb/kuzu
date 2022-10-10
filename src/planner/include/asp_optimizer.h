#pragma once

#include "src/binder/expression/include/node_expression.h"
#include "src/common/include/join_type.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class ASPOptimizer {
public:
    static bool canApplyASP(const vector<shared_ptr<NodeExpression>>& joinNodes, bool isLeftAcc,
        const LogicalPlan& leftPlan, const LogicalPlan& rightPlan);

    static void applyASP(
        const shared_ptr<NodeExpression>& joinNode, LogicalPlan& leftPlan, LogicalPlan& rightPlan);

private:
    static void appendSemiMasker(const shared_ptr<NodeExpression>& node, LogicalPlan& plan);
};

} // namespace planner
} // namespace graphflow
