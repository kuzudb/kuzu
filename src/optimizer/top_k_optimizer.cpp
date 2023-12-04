#include "optimizer/top_k_optimizer.h"

#include "planner/operator/logical_limit.h"
#include "planner/operator/logical_order_by.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void TopKOptimizer::rewrite(planner::LogicalPlan* plan) {
    plan->setLastOperator(visitOperator(plan->getLastOperator()));
}

std::shared_ptr<LogicalOperator> TopKOptimizer::visitOperator(
    const std::shared_ptr<LogicalOperator>& op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    result->computeFlatSchema();
    return result;
}

// TODO(Xiyang): we should probably remove the projection between ORDER BY and MULTIPLICITY REDUCER
// We search for pattern
// ORDER BY -> PROJECTION -> MULTIPLICITY REDUCER -> LIMIT
// and rewrite as TOP_K
std::shared_ptr<LogicalOperator> TopKOptimizer::visitLimitReplace(
    std::shared_ptr<LogicalOperator> op) {
    auto limit = (LogicalLimit*)op.get();
    if (!limit->hasLimitNum()) {
        return op; // only skip no limit. No need to rewrite
    }
    auto multiplicityReducer = limit->getChild(0);
    KU_ASSERT(multiplicityReducer->getOperatorType() == LogicalOperatorType::MULTIPLICITY_REDUCER);
    if (multiplicityReducer->getChild(0)->getOperatorType() != LogicalOperatorType::PROJECTION) {
        return op;
    }
    auto projection = multiplicityReducer->getChild(0);
    if (projection->getChild(0)->getOperatorType() != LogicalOperatorType::ORDER_BY) {
        return op;
    }
    auto orderBy = std::static_pointer_cast<LogicalOrderBy>(projection->getChild(0));
    orderBy->setLimitNum(limit->getLimitNum());
    auto skipNum = limit->hasSkipNum() ? limit->getSkipNum() : 0;
    orderBy->setSkipNum(skipNum);
    return projection;
}

} // namespace optimizer
} // namespace kuzu
