#include "optimizer/remove_factorization_rewriter.h"

#include "common/exception.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void RemoveFactorizationRewriter::rewrite(planner::LogicalPlan* plan) {
    auto root = plan->getLastOperator();
    rewriteOperator(root);
    if (subPlanHasFlatten(root.get())) {
        throw common::InternalException("Remove factorization rewriter failed.");
    }
}

std::shared_ptr<planner::LogicalOperator> RemoveFactorizationRewriter::rewriteOperator(
    std::shared_ptr<planner::LogicalOperator> op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, rewriteOperator(op->getChild(i)));
    }
    if (op->getOperatorType() == planner::LogicalOperatorType::FLATTEN) {
        return op->getChild(0);
    }
    return op;
}

bool RemoveFactorizationRewriter::subPlanHasFlatten(planner::LogicalOperator* op) {
    if (op->getOperatorType() == planner::LogicalOperatorType::FLATTEN) {
        return true;
    }
    for (auto& child : op->getChildren()) {
        if (subPlanHasFlatten(child.get())) {
            return true;
        }
    }
    return false;
}

} // namespace optimizer
} // namespace kuzu
