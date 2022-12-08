#pragma once

#include "binder/expression/node_expression.h"
#include "common/join_type.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
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
} // namespace kuzu
