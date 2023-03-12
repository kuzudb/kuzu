#include "optimizer/asp_optimizer.h"

#include "optimizer/logical_operator_collector.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
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
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return;
    }
    std::vector<LogicalOperator*> buildRoots;
    buildRoots.push_back(hashJoin->getChild(1).get());
    auto scanNodes = resolveScanNodesToApplySemiMask(hashJoin->getJoinNodeIDs(), buildRoots);
    if (scanNodes.empty()) {
        return;
    }
    // apply ASP
    hashJoin->setSIP(SidewaysInfoPassing::LEFT_TO_RIGHT);
    applyASP(scanNodes, op);
}

void ASPOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return;
    }
    std::vector<LogicalOperator*> buildRoots;
    for (auto i = 1; i < intersect->getNumChildren(); ++i) {
        buildRoots.push_back(intersect->getChild(i).get());
    }
    auto scanNodes = resolveScanNodesToApplySemiMask(intersect->getKeyNodeIDs(), buildRoots);
    if (scanNodes.empty()) {
        return;
    }
    intersect->setSIP(SidewaysInfoPassing::LEFT_TO_RIGHT);
    applyASP(scanNodes, op);
}

// Probe side is qualified if it is selective.
bool ASPOptimizer::isProbeSideQualified(planner::LogicalOperator* probeRoot) {
    if (probeRoot->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No ASP if probe side has already been accumulated. This can be solved.
        return false;
    }
    auto filterCollector = LogicalFilterCollector();
    filterCollector.collect(probeRoot);
    if (!filterCollector.hasOperators()) {
        // Probe side is not selective. So we don't apply ASP.
        return false;
    }
    return true;
}

std::vector<planner::LogicalOperator*> ASPOptimizer::resolveScanNodesToApplySemiMask(
    const binder::expression_vector& nodeIDCandidates,
    const std::vector<planner::LogicalOperator*>& buildRoots) {
    binder::expression_map<std::vector<LogicalOperator*>> nodeIDToScanOperatorsMap;
    for (auto& buildRoot : buildRoots) {
        auto scanNodesCollector = LogicalScanNodeCollector();
        scanNodesCollector.collect(buildRoot);
        for (auto& op : scanNodesCollector.getOperators()) {
            auto scanNode = (LogicalScanNode*)op;
            if (scanNode->getNode()->isMultiLabeled()) {
                // We don't push semi mask to multi-labeled scan. This can be solved.
                continue;
            }
            auto nodeID = scanNode->getNode()->getInternalIDProperty();
            if (!nodeIDToScanOperatorsMap.contains(nodeID)) {
                nodeIDToScanOperatorsMap.insert({nodeID, std::vector<LogicalOperator*>{}});
            }
            nodeIDToScanOperatorsMap.at(nodeID).push_back(op);
        }
    }
    // Match node ID candidate with scanNode operators.
    std::vector<LogicalOperator*> result;
    for (auto& nodeID : nodeIDCandidates) {
        if (!nodeIDToScanOperatorsMap.contains(nodeID)) {
            // No scan on the build side to push semi mask to.
            continue;
        }
        if (nodeIDToScanOperatorsMap.at(nodeID).size() > 1) {
            // We don't push semi mask to multiple scans. This can be solved.
            continue;
        }
        result.push_back(nodeIDToScanOperatorsMap.at(nodeID)[0]);
    }
    return result;
}

void ASPOptimizer::applyASP(
    const std::vector<planner::LogicalOperator*>& scanNodes, planner::LogicalOperator* op) {
    auto currentChild = op->getChild(0);
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

} // namespace optimizer
} // namespace kuzu
