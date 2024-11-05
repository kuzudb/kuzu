#include "optimizer/limit_push_down_optimizer.h"

#include "planner/operator/logical_distinct.h"
#include "planner/operator/logical_limit.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void LimitPushDownOptimizer::rewrite(LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void LimitPushDownOptimizer::visitOperator(planner::LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::LIMIT: {
        auto& limit = op->constCast<LogicalLimit>();
        skipNumber = limit.getSkipNum();
        limitNumber = limit.getLimitNum();
        visitOperator(limit.getChild(0).get());
        return;
    }
    case LogicalOperatorType::MULTIPLICITY_REDUCER:
    case LogicalOperatorType::EXPLAIN:
    case LogicalOperatorType::ACCUMULATE:
    case LogicalOperatorType::PATH_PROPERTY_PROBE:
    case LogicalOperatorType::PROJECTION: {
        visitOperator(op->getChild(0).get());
        return;
    }
    case LogicalOperatorType::DISTINCT: {
        if (limitNumber == INVALID_LIMIT && skipNumber == 0) {
            return;
        }
        auto& distinctOp = op->cast<LogicalDistinct>();
        distinctOp.setLimitNum(limitNumber);
        distinctOp.setSkipNum(skipNumber);
        return;
    }
    case LogicalOperatorType::UNION_ALL: {
        for (auto i = 0u; i < op->getNumChildren(); ++i) {
            auto optimizer = LimitPushDownOptimizer();
            optimizer.visitOperator(op->getChild(i).get());
        }
        return;
    }
    default:
        return;
    }
}

} // namespace optimizer
} // namespace kuzu
