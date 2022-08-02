#include "src/processor/include/physical_plan/mapper/physical_plan_optimizer.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

void SemiJoinOptimizer::tryOptimize(PhysicalOperator* op) {
    if (op->getOperatorType() == HASH_JOIN_PROBE) {
        optimize(op);
    }
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        tryOptimize(op->getChild(i));
    }
}

void SemiJoinOptimizer::optimize(PhysicalOperator* op) {
    assert(op->getOperatorType() == HASH_JOIN_PROBE);
    auto scanNodeIDOps = collectScanNodeID(op->getChild(0));
    assert(op->getChild(1)->getOperatorType() == HASH_JOIN_BUILD);
    auto hashJoinBuild = (HashJoinBuild*)op->getChild(1);
    for (auto& scanNodeIDOp : scanNodeIDOps) {
        auto scanNodeID = (ScanNodeID*)scanNodeIDOp;
        auto scanNodeIDSharedState = scanNodeID->getSharedState();
        bool hasSemiJoinMask = scanNodeIDSharedState->getHasSemiMask();
        bool canApplySemiJoinMask = scanNodeID->getNodeID() == hashJoinBuild->getNodeID();
        if (canApplySemiJoinMask && !hasSemiJoinMask) { // config semi join
            scanNodeIDSharedState->setHasSemiMask();
            hashJoinBuild->setScanNodeIDSharedState(scanNodeIDSharedState);
        }
    }
}

vector<PhysicalOperator*> SemiJoinOptimizer::collectScanNodeID(PhysicalOperator* op) {
    vector<PhysicalOperator*> result;
    collectScanNodeIDRecursive(op, result);
    return result;
}

void SemiJoinOptimizer::collectScanNodeIDRecursive(
    PhysicalOperator* op, vector<PhysicalOperator*>& scanNodeIDOps) {
    if (op->getOperatorType() == SCAN_NODE_ID) {
        scanNodeIDOps.push_back(op);
        return;
    }
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        collectScanNodeIDRecursive(op->getChild(i), scanNodeIDOps);
    }
}

} // namespace processor
} // namespace graphflow
