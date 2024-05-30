#include "optimizer/acc_hash_join_optimizer.h"

#include "common/cast.h"
#include "optimizer/logical_operator_collector.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_accumulate.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_intersect.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/operator/sip/logical_semi_masker.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void HashJoinSIPOptimizer::rewrite(LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void HashJoinSIPOptimizer::visitOperator(LogicalOperator* op) {
    // bottom up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void HashJoinSIPOptimizer::visitHashJoin(LogicalOperator* op) {
    auto hashJoin = ku_dynamic_cast<LogicalOperator*, LogicalHashJoin*>(op);
    if (hashJoin->getSIP() == SidewaysInfoPassing::PROHIBIT) {
        return;
    }
    if (hashJoin->getJoinType() != JoinType::INNER) {
        return;
    }
    if (tryBuildToProbeHJSIP(op)) { // Try build to probe SIP first.
        return;
    }
    tryProbeToBuildHJSIP(op);
}

bool HashJoinSIPOptimizer::tryProbeToBuildHJSIP(LogicalOperator* op) {
    auto hashJoin = ku_dynamic_cast<LogicalOperator*, LogicalHashJoin*>(op);
    if (hashJoin->getSIP() == SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
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
    hashJoin->setJoinSubPlanSolveOrder(JoinSubPlanSolveOrder::BUILD_PROBE);
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

bool HashJoinSIPOptimizer::tryBuildToProbeHJSIP(LogicalOperator* op) {
    auto hashJoin = ku_dynamic_cast<LogicalOperator*, LogicalHashJoin*>(op);
    if (hashJoin->getSIP() == SidewaysInfoPassing::PROHIBIT_BUILD_TO_PROBE) {
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
    hashJoin->setSIP(SidewaysInfoPassing::BUILD_TO_PROBE);
    hashJoin->setJoinSubPlanSolveOrder(JoinSubPlanSolveOrder::PROBE_BUILD);
    hashJoin->setChild(1, buildRoot);
    return true;
}

void HashJoinSIPOptimizer::visitIntersect(LogicalOperator* op) {
    auto intersect = ku_dynamic_cast<LogicalOperator*, LogicalIntersect*>(op);
    if (intersect->getSIP() == SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return;
    }
    if (!isProbeSideQualified(op->getChild(0).get())) {
        return;
    }
    auto probeRoot = intersect->getChild(0);
    auto hasSemiMaskApplied = false;
    for (auto& nodeID : intersect->getKeyNodeIDs()) {
        std::vector<LogicalOperator*> ops;
        for (auto i = 1u; i < intersect->getNumChildren(); ++i) {
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

void HashJoinSIPOptimizer::visitPathPropertyProbe(LogicalOperator* op) {
    auto pathPropertyProbe = ku_dynamic_cast<LogicalOperator*, LogicalPathPropertyProbe*>(op);
    if (pathPropertyProbe->getSIP() == SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD) {
        return;
    }
    if (pathPropertyProbe->getJoinType() == RecursiveJoinType::TRACK_NONE) {
        return;
    }
    auto recursiveRel = pathPropertyProbe->getRel();
    auto nodeID = recursiveRel->getRecursiveInfo()->node->getInternalID();
    std::vector<LogicalOperator*> opsToApplySemiMask;
    if (pathPropertyProbe->getNodeChild() != nullptr) {
        auto child = pathPropertyProbe->getNodeChild().get();
        for (auto op_ : resolveOperatorsToApplySemiMask(*nodeID, child)) {
            opsToApplySemiMask.push_back(op_);
        }
    }
    if (pathPropertyProbe->getRelChild() != nullptr) {
        auto child = pathPropertyProbe->getRelChild().get();
        for (auto op_ : resolveOperatorsToApplySemiMask(*nodeID, child)) {
            opsToApplySemiMask.push_back(op_);
        }
    }
    if (opsToApplySemiMask.empty()) {
        return;
    }
    auto semiMask =
        appendPathSemiMasker(recursiveRel, opsToApplySemiMask, pathPropertyProbe->getChild(0));
    pathPropertyProbe->setSIP(SidewaysInfoPassing::PROBE_TO_BUILD);
    pathPropertyProbe->setChild(0, appendAccumulate(semiMask));
}

// Probe side is qualified if it is selective.
bool HashJoinSIPOptimizer::isProbeSideQualified(LogicalOperator* probeRoot) {
    if (probeRoot->getOperatorType() == LogicalOperatorType::ACCUMULATE) {
        // No Acc hash join if probe side has already been accumulated. This can be solved.
        return false;
    }
    // Probe side is not selective. So we don't apply acc hash join.
    return subPlanContainsFilter(probeRoot);
}

std::vector<LogicalOperator*> HashJoinSIPOptimizer::resolveOperatorsToApplySemiMask(
    const Expression& nodeID, LogicalOperator* root) {
    std::vector<LogicalOperator*> result;
    // TODO(Xiyang/Guodong): enable semi mask to ScanNodeTable
    //    for (auto& op : resolveScanInternalIDsToApplySemiMask(nodeID, root)) {
    //        result.push_back(op);
    //    }
    for (auto& op : resolveShortestPathExtendToApplySemiMask(nodeID, root)) {
        result.push_back(op);
    }
    return result;
}

std::vector<LogicalOperator*> HashJoinSIPOptimizer::resolveScanInternalIDsToApplySemiMask(
    const Expression& nodeID, LogicalOperator* root) {
    std::vector<LogicalOperator*> result;
    auto collector = LogicalScanNodeTableCollector();
    collector.collect(root);
    for (auto& op : collector.getOperators()) {
        auto scan = ku_dynamic_cast<LogicalOperator*, LogicalScanNodeTable*>(op);
        if (nodeID.getUniqueName() == scan->getNodeID()->getUniqueName()) {
            result.push_back(op);
        }
    }
    return result;
}

std::vector<LogicalOperator*> HashJoinSIPOptimizer::resolveShortestPathExtendToApplySemiMask(
    const Expression& nodeID, LogicalOperator* root) {
    std::vector<LogicalOperator*> result;
    auto recursiveJoinCollector = LogicalRecursiveExtendCollector();
    recursiveJoinCollector.collect(root);
    for (const auto& op : recursiveJoinCollector.getOperators()) {
        const auto recursiveJoin = ku_dynamic_cast<LogicalOperator*, LogicalRecursiveExtend*>(op);
        const auto node = recursiveJoin->getNbrNode();
        if (nodeID.getUniqueName() == node->getInternalID()->getUniqueName()) {
            result.push_back(op);
            return result;
        }
    }
    return result;
}

std::shared_ptr<LogicalOperator> HashJoinSIPOptimizer::appendNodeSemiMasker(
    std::vector<LogicalOperator*> opsToApplySemiMask, std::shared_ptr<LogicalOperator> child) {
    KU_ASSERT(!opsToApplySemiMask.empty());
    auto op = opsToApplySemiMask[0];
    std::shared_ptr<Expression> key;
    std::vector<table_id_t> nodeTableIDs;
    switch (op->getOperatorType()) {
    case LogicalOperatorType::SCAN_NODE_TABLE: {
        const auto scan = ku_dynamic_cast<LogicalOperator*, LogicalScanNodeTable*>(op);
        key = scan->getNodeID();
        nodeTableIDs = scan->getTableIDs();
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        const auto extend = ku_dynamic_cast<LogicalOperator*, LogicalRecursiveExtend*>(op);
        key = extend->getNbrNode()->getInternalID();
        nodeTableIDs = extend->getNbrNode()->getTableIDs();
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto semiMasker = std::make_shared<LogicalSemiMasker>(SemiMaskType::NODE, std::move(key),
        std::move(nodeTableIDs), opsToApplySemiMask, std::move(child));
    semiMasker->computeFlatSchema();
    return semiMasker;
}

std::shared_ptr<LogicalOperator> HashJoinSIPOptimizer::appendPathSemiMasker(
    const std::shared_ptr<Expression>& pathExpression,
    std::vector<LogicalOperator*> opsToApplySemiMask,
    const std::shared_ptr<LogicalOperator>& child) {
    KU_ASSERT(!opsToApplySemiMask.empty());
    const auto op = opsToApplySemiMask[0];
    KU_ASSERT(op->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE);
    const auto scan = ku_dynamic_cast<LogicalOperator*, LogicalScanNodeTable*>(op);
    auto semiMasker = std::make_shared<LogicalSemiMasker>(SemiMaskType::PATH, pathExpression,
        scan->getTableIDs(), opsToApplySemiMask, child);
    semiMasker->computeFlatSchema();
    return semiMasker;
}

std::shared_ptr<LogicalOperator> HashJoinSIPOptimizer::appendAccumulate(
    std::shared_ptr<LogicalOperator> child) {
    auto accumulate = std::make_shared<LogicalAccumulate>(AccumulateType::REGULAR,
        expression_vector{}, nullptr /* offset */, nullptr /* mark */, std::move(child));
    accumulate->computeFlatSchema();
    return accumulate;
}

} // namespace optimizer
} // namespace kuzu
