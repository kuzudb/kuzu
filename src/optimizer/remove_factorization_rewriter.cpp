#include "optimizer/remove_factorization_rewriter.h"

#include "common/exception.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void RemoveFactorizationRewriter::rewrite(planner::LogicalPlan* plan) {
    auto root = plan->getLastOperator();
    visitOperator(root);
    auto verifier = Verifier();
    verifier.visit(root.get());
    if (verifier.containsFlatten()) {
        throw common::InternalException("Remove factorization rewriter failed.");
    }
}

std::shared_ptr<planner::LogicalOperator> RemoveFactorizationRewriter::visitOperator(
    std::shared_ptr<planner::LogicalOperator> op) {
    // bottom-up traversal
    for (auto i = 0; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    //    op->getSchema()->clear();
    return visitOperatorReplaceSwitch(op);
}

std::shared_ptr<planner::LogicalOperator> RemoveFactorizationRewriter::visitFlattenReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
    return op->getChild(0);
}

void RemoveFactorizationRewriter::Verifier::visit(planner::LogicalOperator* op) {
    for (auto i = 0; i < op->getNumChildren(); ++i) {
        visit(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void RemoveFactorizationRewriter::Verifier::visitFlatten(planner::LogicalOperator* op) {
    containsFlatten_ = true;
}

} // namespace optimizer
} // namespace kuzu
