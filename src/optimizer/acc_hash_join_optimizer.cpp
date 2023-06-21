#include "optimizer/acc_hash_join_optimizer.h"

#include "optimizer/logical_operator_collector.h"
#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void HashJoinSIPOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void HashJoinSIPOptimizer::visitOperator(planner::LogicalOperator* op) {
    // bottom up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void HashJoinSIPOptimizer::visitHashJoin(planner::LogicalOperator* op) {
    if (tryBuildToProbeHJSIP(op)) { // Try build to probe SIP first.
        return;
    }
    tryProbeToBuildHJSIP(op);
}

bool HashJoinSIPOptimizer::tryProbeToBuildHJSIP(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    if (hashJoin->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return false;
    }
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return false;
    }
    auto probeRoot = hashJoin->getChild(0);
    auto buildRoot = hashJoin->getChild(1);
    auto hasSemiMaskApplied = false;
    for (auto& nodeID : hashJoin->getJoinNodeIDs()) {
        auto ops = resolveOperatorsToApplySemiMask(*nodeID, buildRoot.get());
        if (!ops.empty()) {
            probeRoot = appendNodeSemiMasker(ops, probeRoot);
            hasSemiMaskApplied = true;
        }
    }
    if (!hasSemiMaskApplied) {
        return false;
    }
    hashJoin->setSIP(SidewaysInfoPassing::PROBE_TO_BUILD);
    hashJoin->setChild(0, appendAccumulate(probeRoot));
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

bool HashJoinSIPOptimizer::tryBuildToProbeHJSIP(planner::LogicalOperator* op) {
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
    auto probeRoot = hashJoin->getChild(0);
    auto buildRoot = hashJoin->getChild(1);
    for (auto& nodeID : hashJoin->getJoinNodeIDs()) {
        auto ops = resolveOperatorsToApplySemiMask(*nodeID, probeRoot.get());
        if (!ops.empty()) {
            buildRoot = appendNodeSemiMasker(ops, buildRoot);
        }
    }
    hashJoin->setSIP(planner::SidewaysInfoPassing::BUILD_TO_PROBE);
    hashJoin->setChild(1, buildRoot);
    return true;
}

void HashJoinSIPOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    if (intersect->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return;
    }
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return;
    }
    auto probeRoot = intersect->getChild(0);
    auto hasSemiMaskApplied = false;
    for (auto& nodeID : intersect->getKeyNodeIDs()) {
        std::vector<planner::LogicalOperator*> ops;
        for (auto i = 1; i < intersect->getNumChildren(); ++i) {
            auto buildRoot = intersect->getChild(1);
            for (auto& op_ : resolveOperatorsToApplySemiMask(*nodeID, buildRoot.get())) {
                ops.push_back(op_);
            }
        }
        if (!ops.empty()) {
            probeRoot = appendNodeSemiMasker(ops, probeRoot);
            hasSemiMaskApplied = true;
        }
    }
    if (!hasSemiMaskApplied) {
        return;
    }
    intersect->setSIP(SidewaysInfoPassing::PROBE_TO_BUILD);
    intersect->setChild(0, appendAccumulate(probeRoot));
}

void HashJoinSIPOptimizer::visitPathPropertyProbe(planner::LogicalOperator* op) {
    auto pathPropertyProbe = (LogicalPathPropertyProbe*)op;
    if (pathPropertyProbe->getSIP() == planner::SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return;
    }
    if (pathPropertyProbe->getNumChildren() == 1) { // No path being tracked.
        return;
    }
    auto recursiveRel = pathPropertyProbe->getRel();
    auto nodeID = recursiveRel->getRecursiveInfo()->node->getInternalIDProperty();
    std::vector<LogicalOperator*> opsToApplySemiMask;
    for (auto op_ :
        resolveOperatorsToApplySemiMask(*nodeID, pathPropertyProbe->getChild(1).get())) {
        opsToApplySemiMask.push_back(op_);
    }
    for (auto op_ :
        resolveOperatorsToApplySemiMask(*nodeID, pathPropertyProbe->getChild(2).get())) {
        opsToApplySemiMask.push_back(op_);
    }
    assert(opsToApplySemiMask.size() == 2);
    auto semiMask =
        appendPathSemiMasker(recursiveRel, opsToApplySemiMask, pathPropertyProbe->getChild(0));
    pathPropertyProbe->setSIP(planner::SidewaysInfoPassing::PROBE_TO_BUILD);
    pathPropertyProbe->setChild(0, appendAccumulate(semiMask));
}

// Probe side is qualified if it is selective.
bool HashJoinSIPOptimizer::isProbeSideQualified(planner::LogicalOperator* probeRoot) {
    if (probeRoot->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No Acc hash join if probe side has already been accumulated. This can be solved.
        return false;
    }
    // Probe side is not selective. So we don't apply acc hash join.
    return subPlanContainsFilter(probeRoot);
}

std::vector<planner::LogicalOperator*> HashJoinSIPOptimizer::resolveOperatorsToApplySemiMask(
    const binder::Expression& nodeID, planner::LogicalOperator* root) {
    std::vector<planner::LogicalOperator*> result;
    for (auto& op : resolveScanNodeIDsToApplySemiMask(nodeID, root)) {
        result.push_back(op);
    }
    for (auto& op : resolveShortestPathExtendToApplySemiMask(nodeID, root)) {
        result.push_back(op);
    }
    return result;
}

std::vector<planner::LogicalOperator*> HashJoinSIPOptimizer::resolveScanNodeIDsToApplySemiMask(
    const binder::Expression& nodeID, planner::LogicalOperator* root) {
    std::vector<planner::LogicalOperator*> result;
    auto scanNodesCollector = LogicalScanNodeCollector();
    scanNodesCollector.collect(root);
    for (auto& op : scanNodesCollector.getOperators()) {
        auto scanNode = (LogicalScanNode*)op;
        auto node = scanNode->getNode();
        if (nodeID.getUniqueName() == node->getInternalIDProperty()->getUniqueName()) {
            result.push_back(op);
        }
    }
    return result;
}

std::vector<planner::LogicalOperator*>
HashJoinSIPOptimizer::resolveShortestPathExtendToApplySemiMask(
    const binder::Expression& nodeID, planner::LogicalOperator* root) {
    std::vector<planner::LogicalOperator*> result;
    auto recursiveJoinCollector = LogicalRecursiveExtendCollector();
    recursiveJoinCollector.collect(root);
    for (auto& op : recursiveJoinCollector.getOperators()) {
        auto recursiveJoin = (LogicalRecursiveExtend*)op;
        auto node = recursiveJoin->getNbrNode();
        if (nodeID.getUniqueName() == node->getInternalIDProperty()->getUniqueName()) {
            result.push_back(op);
            return result;
        }
    }
    return result;
}

std::shared_ptr<planner::LogicalOperator> HashJoinSIPOptimizer::appendNodeSemiMasker(
    std::vector<planner::LogicalOperator*> opsToApplySemiMask,
    std::shared_ptr<planner::LogicalOperator> child) {
    assert(!opsToApplySemiMask.empty());
    auto op = opsToApplySemiMask[0];
    std::shared_ptr<binder::NodeExpression> node;
    switch (op->getOperatorType()) {
    case LogicalOperatorType::SCAN_NODE: {
        auto scanNode = (LogicalScanNode*)op;
        node = scanNode->getNode();
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        auto recursiveExtend = (LogicalRecursiveExtend*)op;
        node = recursiveExtend->getNbrNode();
    } break;
    default:
        throw common::NotImplementedException("HashJoinSIPOptimizer::appendSemiMask");
    }
    auto semiMasker = std::make_shared<LogicalSemiMasker>(
        SemiMaskType::NODE, node->getInternalIDProperty(), node, opsToApplySemiMask, child);
    semiMasker->computeFlatSchema();
    return semiMasker;
}

std::shared_ptr<planner::LogicalOperator> HashJoinSIPOptimizer::appendPathSemiMasker(
    std::shared_ptr<binder::Expression> pathExpression,
    std::vector<planner::LogicalOperator*> opsToApplySemiMask,
    std::shared_ptr<planner::LogicalOperator> child) {
    assert(!opsToApplySemiMask.empty());
    auto op = opsToApplySemiMask[0];
    assert(op->getOperatorType() == planner::LogicalOperatorType::SCAN_NODE);
    auto scanNode = (LogicalScanNode*)op;
    auto node = scanNode->getNode();
    auto semiMasker = std::make_shared<LogicalSemiMasker>(
        SemiMaskType::PATH, pathExpression, node, opsToApplySemiMask, child);
    semiMasker->computeFlatSchema();
    return semiMasker;
}

std::shared_ptr<planner::LogicalOperator> HashJoinSIPOptimizer::appendAccumulate(
    std::shared_ptr<planner::LogicalOperator> child) {
    auto accumulate = std::make_shared<LogicalAccumulate>(std::move(child));
    accumulate->computeFlatSchema();
    return accumulate;
}

} // namespace optimizer
} // namespace kuzu
