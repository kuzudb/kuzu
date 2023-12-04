#include "optimizer/remove_unnecessary_join_optimizer.h"

#include "planner/operator/logical_hash_join.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void RemoveUnnecessaryJoinOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator());
}

std::shared_ptr<planner::LogicalOperator> RemoveUnnecessaryJoinOptimizer::visitOperator(
    const std::shared_ptr<planner::LogicalOperator>& op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    result->computeFlatSchema();
    return result;
}

std::shared_ptr<planner::LogicalOperator> RemoveUnnecessaryJoinOptimizer::visitHashJoinReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
    auto hashJoin = (LogicalHashJoin*)op.get();
    switch (hashJoin->getJoinType()) {
    case JoinType::MARK:
    case JoinType::LEFT: {
        // Do not prune no-trivial join type
        return op;
    }
    default:
        break;
    }
    if (op->getChild(1)->getOperatorType() == LogicalOperatorType::SCAN_INTERNAL_ID) {
        // Build side is trivial. Prune build side.
        return op->getChild(0);
    }
    if (op->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_INTERNAL_ID) {
        // Probe side is trivial. Prune probe side.
        return op->getChild(1);
    }
    return op;
}

} // namespace optimizer
} // namespace kuzu
