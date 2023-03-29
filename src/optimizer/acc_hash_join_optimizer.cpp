#include "optimizer/acc_hash_join_optimizer.h"

#include "optimizer/logical_operator_collector.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void AccHashJoinOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void AccHashJoinOptimizer::visitOperator(planner::LogicalOperator* op) {
    // bottom up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void AccHashJoinOptimizer::visitHashJoin(planner::LogicalOperator* op) {
    if (tryProbeToBuildHJSIP(op)) { // Try probe to build SIP first.
        return;
    }
    tryBuildToProbeHJSIP(op);
}

bool AccHashJoinOptimizer::tryProbeToBuildHJSIP(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    if (hashJoin->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return false;
    }
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return false;
    }
    std::vector<LogicalOperator*> buildRoots;
    buildRoots.push_back(hashJoin->getChild(1).get());
    auto scanNodes = resolveScanNodesToApplySemiMask(hashJoin->getJoinNodeIDs(), buildRoots);
    if (scanNodes.empty()) {
        return false;
    }
    // apply accumulated hash join
    hashJoin->setSIP(SidewaysInfoPassing::PROBE_TO_BUILD);
    applyAccHashJoin(scanNodes, op);
    return true;
}

static bool subPlanContainsFilter(LogicalOperator* root) {
    auto filterCollector = LogicalFilterCollector();
    filterCollector.collect(root);
    auto indexScanNodeCollector = LogicalIndexScanNodeCollector();
    indexScanNodeCollector.collect(root);
    if (!filterCollector.hasOperators() && !indexScanNodeCollector.hasOperators()) {
        return false;
    }
    return true;
}

bool AccHashJoinOptimizer::tryBuildToProbeHJSIP(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    if (hashJoin->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_BUILD_TO_PROBE) {
        return false;
    }
    if (hashJoin->getJoinType() != common::JoinType::INNER) {
        return false;
    }
    if (!subPlanContainsFilter(hashJoin->getChild(1).get())) {
        return false;
    }
    std::vector<LogicalOperator*> roots;
    roots.push_back(hashJoin->getChild(0).get());
    auto scanNodes = resolveScanNodesToApplySemiMask(hashJoin->getJoinNodeIDs(), roots);
    if (scanNodes.empty()) {
        return false;
    }
    hashJoin->setSIP(planner::SidewaysInfoPassing::BUILD_TO_PROBE);
    hashJoin->setChild(1, applySemiMasks(scanNodes, op->getChild(1)));
    return true;
}

void AccHashJoinOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    if (intersect->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return;
    }
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
    intersect->setSIP(SidewaysInfoPassing::PROBE_TO_BUILD);
    applyAccHashJoin(scanNodes, op);
}

// Probe side is qualified if it is selective.
bool AccHashJoinOptimizer::isProbeSideQualified(planner::LogicalOperator* probeRoot) {
    if (probeRoot->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No Acc hash join if probe side has already been accumulated. This can be solved.
        return false;
    }
    // Probe side is not selective. So we don't apply acc hash join.
    return subPlanContainsFilter(probeRoot);
}

binder::expression_map<std::vector<planner::LogicalOperator*>>
AccHashJoinOptimizer::resolveScanNodesToApplySemiMask(
    const binder::expression_vector& nodeIDCandidates,
    const std::vector<planner::LogicalOperator*>& roots) {
    binder::expression_map<std::vector<LogicalOperator*>> nodeIDToScanOperatorsMap;
    for (auto& root : roots) {
        auto scanNodesCollector = LogicalScanNodeCollector();
        scanNodesCollector.collect(root);
        for (auto& op : scanNodesCollector.getOperators()) {
            auto scanNode = (LogicalScanNode*)op;
            auto nodeID = scanNode->getNode()->getInternalIDProperty();
            if (!nodeIDToScanOperatorsMap.contains(nodeID)) {
                nodeIDToScanOperatorsMap.insert({nodeID, std::vector<LogicalOperator*>{}});
            }
            nodeIDToScanOperatorsMap.at(nodeID).push_back(op);
        }
    }
    // Match node ID candidate with scanNode operators.
    binder::expression_map<std::vector<planner::LogicalOperator*>> result;
    for (auto& nodeID : nodeIDCandidates) {
        if (!nodeIDToScanOperatorsMap.contains(nodeID)) {
            // No scan on the build side to push semi mask to.
            continue;
        }
        result.insert({nodeID, nodeIDToScanOperatorsMap.at(nodeID)});
    }
    return result;
}

std::shared_ptr<planner::LogicalOperator> AccHashJoinOptimizer::applySemiMasks(
    const binder::expression_map<std::vector<planner::LogicalOperator*>>& nodeIDToScanNodes,
    std::shared_ptr<planner::LogicalOperator> root) {
    auto currentRoot = root;
    for (auto& [nodeID, scanNodes] : nodeIDToScanNodes) {
        auto semiMasker = std::make_shared<LogicalSemiMasker>(nodeID, scanNodes, currentRoot);
        semiMasker->computeFlatSchema();
        currentRoot = semiMasker;
    }
    return currentRoot;
}

void AccHashJoinOptimizer::applyAccHashJoin(
    const binder::expression_map<std::vector<planner::LogicalOperator*>>& nodeIDToScanNodes,
    planner::LogicalOperator* op) {
    auto currentRoot = applySemiMasks(nodeIDToScanNodes, op->getChild(0));
    auto accumulate = std::make_shared<LogicalAccumulate>(std::move(currentRoot));
    accumulate->computeFlatSchema();
    op->setChild(0, std::move(accumulate));
}

} // namespace optimizer
} // namespace kuzu
