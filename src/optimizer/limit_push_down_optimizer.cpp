#include "optimizer/limit_push_down_optimizer.h"

#include "planner/operator/logical_distinct.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void LimitPushDownOptimizer::rewrite(LogicalPlan* plan) {
    plan->setLastOperator(visitOperator(plan->getLastOperator()));
}

std::shared_ptr<LogicalOperator> LimitPushDownOptimizer::finishPushDown(
    std::shared_ptr<LogicalOperator> op) {
    if (limitOperator == nullptr) {
        return op;
    }
    LogicalPlan plan;
    plan.setLastOperator(op);
    planner::Planner planner(context);
    planner.appendMultiplicityReducer(plan);
    planner.appendLimit(limitOperator->getSkipNum(), limitOperator->getLimitNum(), plan);
    limitOperator = nullptr;
    return plan.getLastOperator();
}

std::shared_ptr<LogicalOperator> LimitPushDownOptimizer::visitOperator(
    std::shared_ptr<LogicalOperator> op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::LIMIT: {
        KU_ASSERT(this->limitOperator == nullptr);
        this->limitOperator = &op->cast<LogicalLimit>();
        KU_ASSERT(op->getChild(0)->getOperatorType() == LogicalOperatorType::MULTIPLICITY_REDUCER);
        // Remove limit
        return visitOperator(op->getChild(0)->getChild(0));
    }
    case LogicalOperatorType::EXPLAIN:
    case LogicalOperatorType::SEMI_MASKER:
    case LogicalOperatorType::ACCUMULATE:
    case LogicalOperatorType::PATH_PROPERTY_PROBE:
    case LogicalOperatorType::PROJECTION: {
        op->setChild(0, visitOperator(op->getChild(0)));
        return op;
    }
    case LogicalOperatorType::DISTINCT: {
        if (limitOperator == nullptr) {
            return op;
        }
        // AGGREGATE's limit is task-level restrictions
        auto& distinctOp = op->cast<LogicalDistinct>();
        distinctOp.setLimitNum(limitOperator->getLimitNum());
        distinctOp.setSkipNum(limitOperator->getSkipNum());
        // We can't remove this limit, because there can be multiple tasks performing AGGREGATE
        return finishPushDown(op);
    }
    case LogicalOperatorType::UNION_ALL: {
        for (auto i = 0u; i < op->getNumChildren(); ++i) {
            auto optimizer = LimitPushDownOptimizer(context);
            op->setChild(i, optimizer.visitOperator(op->getChild(i)));
        }
        return op;
    }
    default: {
        return finishPushDown(op);
    }
    }
}

} // namespace optimizer
} // namespace kuzu
