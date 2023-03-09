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
    if (hashJoin->getChild(0)->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No ASP if probe side has already been accumulated. This can be solved.
        return;
    }
    auto probeSideFilterCollector = LogicalFilterCollector();
    probeSideFilterCollector.collect(op->getChild(0).get());
    if (!probeSideFilterCollector.hasOperators()) {
        // Probe side is not selective so we don't apply ASP.
        return;
    }
    auto scanNodes = resolveScanNodesToApplySemiMask(op);
    if (scanNodes.empty()) {
        return;
    }
    // apply ASP
    hashJoin->setInfoPassing(planner::HashJoinSideWayInfoPassing::LEFT_TO_RIGHT);
    auto currentChild = hashJoin->getChild(0);
    for (auto& op_ : scanNodes) {
        auto scanNode = (LogicalScanNode*)op_;
        auto semiMasker = std::make_shared<LogicalSemiMasker>(scanNode, currentChild);
        semiMasker->computeFlatSchema();
        currentChild = semiMasker;
    }
    auto accumulate = std::make_shared<LogicalAccumulate>(std::move(currentChild));
    accumulate->computeFlatSchema();
    op->setChild(0, std::move(accumulate));
}

std::vector<planner::LogicalOperator*> ASPOptimizer::resolveScanNodesToApplySemiMask(
    planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    binder::expression_map<std::vector<LogicalOperator*>> nodeIDToScanOperatorsMap;
    auto buildSideScanNodesCollector = LogicalScanNodeCollector();
    buildSideScanNodesCollector.collect(op->getChild(1).get());
    // populate node ID to scan operator map
    for (auto& op_ : buildSideScanNodesCollector.getOperators()) {
        auto scanNode = (LogicalScanNode*)op_;
        if (scanNode->getNode()->isMultiLabeled()) {
            // We don't push semi mask to multi-labeled scan. This can be solved.
            continue;
        }
        auto nodeID = scanNode->getNode()->getInternalIDProperty();
        if (!nodeIDToScanOperatorsMap.contains(nodeID)) {
            nodeIDToScanOperatorsMap.insert({nodeID, std::vector<LogicalOperator*>{}});
        }
        nodeIDToScanOperatorsMap.at(nodeID).push_back(op_);
    }
    // generate semi mask info
    std::vector<LogicalOperator*> result;
    for (auto& joinNodeID : hashJoin->getJoinNodeIDs()) {
        if (!nodeIDToScanOperatorsMap.contains(joinNodeID)) {
            // No scan on the build side to push semi mask to.
            continue;
        }
        if (nodeIDToScanOperatorsMap.at(joinNodeID).size() > 1) {
            // We don't push semi mask to multiple scans. This can be solved.
            continue;
        }
        result.push_back(nodeIDToScanOperatorsMap.at(joinNodeID)[0]);
    }
    return result;
}

} // namespace optimizer
} // namespace kuzu
