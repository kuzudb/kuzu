#include "optimizer/asp_optimizer.h"

#include "optimizer/logical_operator_collector.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void ASPOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void ASPOptimizer::visitOperator(planner::LogicalOperator* op) {
    // bottom up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void ASPOptimizer::visitHashJoin(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    if (!canApplyASP(op)) {
        return;
    }
    auto joinNodeID = hashJoin->getJoinNodeIDs()[0];
    // apply ASP
    hashJoin->setInfoPassing(planner::HashJoinSideWayInfoPassing::LEFT_TO_RIGHT);
    auto semiMasker = std::make_shared<LogicalSemiMasker>(joinNodeID, op->getChild(0));
    semiMasker->computeFlatSchema();
    auto accumulate = std::make_shared<LogicalAccumulate>(std::move(semiMasker));
    accumulate->computeFlatSchema();
    op->setChild(0, std::move(accumulate));
}

bool ASPOptimizer::canApplyASP(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    // TODO(Xiyang): solve the cases where we cannot apply ASP.
    if (hashJoin->getJoinNodeIDs().size() > 1) {
        // No ASP for multiple join keys. This can be solved.
        return false;
    }
    auto joinNodeID = hashJoin->getJoinNodeIDs()[0];
    if (hashJoin->getChild(0)->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No ASP if probe side has already been accumulated. This can be solved.
        return false;
    }
    auto probeSideFilterCollector = LogicalFilterCollector();
    probeSideFilterCollector.collect(op->getChild(0).get());
    if (!probeSideFilterCollector.hasOperators()) {
        // Probe side is not selective.
        return false;
    }
    auto buildSideScanNodesCollector = LogicalScanNodeCollector();
    buildSideScanNodesCollector.collect(op->getChild(1).get());
    auto buildSideScanNodes = buildSideScanNodesCollector.getOperators();
    if (buildSideScanNodes.size() != 1) {
        // No ASP if we try to apply semi mask to multiple scan nodes. This can be solved.
        return false;
    }
    auto buildSideNode = ((LogicalScanNode*)buildSideScanNodes[0])->getNode();
    if (buildSideNode->isMultiLabeled()) {
        // No ASP for multi-labeled scan. This can be solved.
        return false;
    }
    if (joinNodeID->getUniqueName() != buildSideNode->getInternalIDPropertyName()) {
        // We only push semi mask to scan nodes.
        return false;
    }
    return true;
}

} // namespace optimizer
} // namespace kuzu
