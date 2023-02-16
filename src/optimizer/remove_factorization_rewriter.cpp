#include "optimizer/remove_factorization_rewriter.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void RemoveFactorizationRewriter::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void RemoveFactorizationRewriter::visitOperator(planner::LogicalOperator* op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    switch (op->getOperatorType()) {
    case LogicalOperatorType::EXTEND: {
        visitExtend(op);
    } break;
    default:
        break;
    }
    op->getSchema()->clear();
}

void RemoveFactorizationRewriter::visitExtend(planner::LogicalOperator* op) {
    auto newChild = getNonFlattenChild(op);
    op->setChild(0, newChild);
}

std::shared_ptr<planner::LogicalOperator> RemoveFactorizationRewriter::getNonFlattenChild(
    planner::LogicalOperator* op) {
    auto currentOp = op->getChild(0);
    while (currentOp->getOperatorType() == LogicalOperatorType::FLATTEN) {
        currentOp = currentOp->getChild(0);
    }
    return currentOp;
}

} // namespace optimizer
} // namespace kuzu
