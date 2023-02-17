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
        op->setChild(0, getNonFlattenOp(op->getChild(0)));
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        op->setChild(0, getNonFlattenOp(op->getChild(0)));
//        op->setChild(1, getNonFlattenOp(op->getChild(1)));
    } break;
    default:
        break;
    }
    op->getSchema()->clear();
}

std::shared_ptr<planner::LogicalOperator> RemoveFactorizationRewriter::getNonFlattenOp(std::shared_ptr<planner::LogicalOperator> op) {
    auto currentOp = op;
    while (currentOp->getOperatorType() == LogicalOperatorType::FLATTEN) {
        currentOp = currentOp->getChild(0);
    }
    return currentOp;
}

} // namespace optimizer
} // namespace kuzu
