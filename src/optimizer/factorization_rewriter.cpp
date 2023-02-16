#include "optimizer/factorization_rewriter.h"

#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_flatten.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void FactorizationRewriter::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void FactorizationRewriter::visitOperator(planner::LogicalOperator* op) {
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
    op->computeSchema();
}

void FactorizationRewriter::visitExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalExtend*)op;
    if (!extend->requireFlatInput()) {
        return;
    }
    auto childSchema = extend->getChild(0)->getSchema();
    auto groupPosToFlatten = extend->getGroupPosToFlatten();
    if (childSchema->getGroup(groupPosToFlatten)->isFlat()) {
        return;
    }
    auto expression = childSchema->getExpressionsInScope(groupPosToFlatten)[0];
    // TODO: refactor as function
    auto flatten = std::make_shared<LogicalFlatten>(expression, extend->getChild(0));
    flatten->computeSchema();
    extend->setChild(0, std::move(flatten));
    extend->computeSchema();
}

} // namespace optimizer
} // namespace kuzu
