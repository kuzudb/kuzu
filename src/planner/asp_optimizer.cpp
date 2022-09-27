#include "include/asp_optimizer.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/include/logical_plan_util.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/planner/logical_plan/logical_operator/include/logical_semi_masker.h"

namespace graphflow {
namespace planner {

bool ASPOptimizer::canApplyASP(shared_ptr<NodeExpression>& joinNode, bool isLeftAcc,
    const LogicalPlan& leftPlan, const LogicalPlan& rightPlan) {
    // Avoid ASP if left side is already accumulated. This is due to simplicity on the mapper side.
    if (isLeftAcc) {
        return false;
    }
    auto isLeftPlanFiltered = !LogicalPlanUtil::collectOperators(leftPlan, LOGICAL_FILTER).empty();
    // ASP join benefits only when left branch is selective.
    if (!isLeftPlanFiltered) {
        return false;
    }
    auto rightScanNodeIDs = LogicalPlanUtil::collectOperators(rightPlan, LOGICAL_SCAN_NODE_ID);
    // TODO(Xiyang): multiple scan node IDs probably also works, but let's do a separate PR.
    if (rightScanNodeIDs.size() != 1) {
        return false;
    }
    auto rightScanNodeID = (LogicalScanNodeID*)rightScanNodeIDs[0];
    // Semi mask can only be pushed to ScanNodeIDs.
    if (joinNode->getUniqueName() != rightScanNodeID->getNode()->getUniqueName()) {
        return false;
    }
    return true;
}

void ASPOptimizer::applyASP(
    shared_ptr<NodeExpression>& joinNode, LogicalPlan& leftPlan, LogicalPlan& rightPlan) {
    appendSemiMasker(joinNode, leftPlan);
    Enumerator::appendAccumulate(leftPlan);
}

void ASPOptimizer::appendSemiMasker(shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    auto semiMasker = make_shared<LogicalSemiMasker>(node, plan.getLastOperator());
    plan.appendOperator(std::move(semiMasker));
}

} // namespace planner
} // namespace graphflow
