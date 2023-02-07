#include "planner/asp_optimizer.h"

#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

bool ASPOptimizer::canApplyASP(const expression_vector& joinNodeIDs, bool isLeftAcc,
    const LogicalPlan& leftPlan, const LogicalPlan& rightPlan) {
    // Avoid ASP if there are multiple join nodes, or the left side is already accumulated, which is
    // due to simplicity on the mapper side.
    if (joinNodeIDs.size() > 1 || isLeftAcc) {
        return false;
    }
    auto isLeftPlanFiltered =
        !LogicalPlanUtil::collectOperators(leftPlan, LogicalOperatorType::FILTER).empty();
    // ASP join benefits only when left branch is selective.
    if (!isLeftPlanFiltered) {
        return false;
    }
    auto rightScanNodeIDs =
        LogicalPlanUtil::collectOperators(rightPlan, LogicalOperatorType::SCAN_NODE);
    // TODO(Xiyang): multiple scan node IDs probably also works, but let's do a separate PR.
    if (rightScanNodeIDs.size() != 1) {
        return false;
    }
    auto rightScanNodeID = (LogicalScanNode*)rightScanNodeIDs[0];
    // Semi mask cannot be applied to a ScanNodeID on multiple node tables.
    if (rightScanNodeID->getNode()->isMultiLabeled()) {
        return false;
    }
    // Semi mask can only be pushed to ScanNodeIDs.
    if (joinNodeIDs[0]->getUniqueName() !=
        rightScanNodeID->getNode()->getInternalIDPropertyName()) {
        return false;
    }
    return true;
}

void ASPOptimizer::applyASP(
    const std::shared_ptr<Expression>& joinNodeID, LogicalPlan& leftPlan, LogicalPlan& rightPlan) {
    appendSemiMasker(joinNodeID, leftPlan);
    QueryPlanner::appendAccumulate(leftPlan);
}

void ASPOptimizer::appendSemiMasker(const std::shared_ptr<Expression>& nodeID, LogicalPlan& plan) {
    auto semiMasker = make_shared<LogicalSemiMasker>(nodeID, plan.getLastOperator());
    semiMasker->computeSchema();
    plan.setLastOperator(std::move(semiMasker));
}

} // namespace planner
} // namespace kuzu
