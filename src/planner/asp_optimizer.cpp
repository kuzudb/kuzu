#include "planner/asp_optimizer.h"

#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

bool ASPOptimizer::canApplyASP(const vector<shared_ptr<NodeExpression>>& joinNodes, bool isLeftAcc,
    const LogicalPlan& leftPlan, const LogicalPlan& rightPlan) {
    // Avoid ASP if there are multiple join nodes, or the left side is already accumulated, which is
    // due to simplicity on the mapper side.
    if (joinNodes.size() > 1 || isLeftAcc) {
        return false;
    }
    auto isLeftPlanFiltered = !LogicalPlanUtil::collectOperators(leftPlan, LOGICAL_FILTER).empty();
    // ASP join benefits only when left branch is selective.
    if (!isLeftPlanFiltered) {
        return false;
    }
    auto rightScanNodeIDs = LogicalPlanUtil::collectOperators(rightPlan, LOGICAL_SCAN_NODE);
    // TODO(Xiyang): multiple scan node IDs probably also works, but let's do a separate PR.
    if (rightScanNodeIDs.size() != 1) {
        return false;
    }
    auto rightScanNodeID = (LogicalScanNode*)rightScanNodeIDs[0];
    // Semi mask can only be pushed to ScanNodeIDs.
    if (joinNodes[0]->getUniqueName() != rightScanNodeID->getNode()->getUniqueName()) {
        return false;
    }
    return true;
}

void ASPOptimizer::applyASP(
    const shared_ptr<NodeExpression>& joinNode, LogicalPlan& leftPlan, LogicalPlan& rightPlan) {
    appendSemiMasker(joinNode, leftPlan);
    QueryPlanner::appendAccumulate(leftPlan);
}

void ASPOptimizer::appendSemiMasker(const shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    auto semiMasker = make_shared<LogicalSemiMasker>(node, plan.getLastOperator());
    plan.setLastOperator(std::move(semiMasker));
}

} // namespace planner
} // namespace kuzu
