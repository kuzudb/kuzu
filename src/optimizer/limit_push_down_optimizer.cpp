#include "optimizer/limit_push_down_optimizer.h"

#include "common/exception/runtime.h"
#include "planner/operator/logical_distinct.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/logical_hash_join.h"
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
        if (limit.hasSkipNum()) {
            skipNumber = limit.getSkipNum();
        }
        if (limit.hasLimitNum()) {
            limitNumber = limit.getLimitNum();
        }
        visitOperator(limit.getChild(0).get());
        return;
    }
    case LogicalOperatorType::MULTIPLICITY_REDUCER:
    case LogicalOperatorType::EXPLAIN:
    case LogicalOperatorType::ACCUMULATE:
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
    case LogicalOperatorType::HASH_JOIN: {
        if (limitNumber == INVALID_LIMIT && skipNumber == 0) {
            return;
        }
        if (op->getChild(0)->getOperatorType() == LogicalOperatorType::HASH_JOIN) {
            op->ptrCast<LogicalHashJoin>()->getSIPInfoUnsafe().position = SemiMaskPosition::NONE;
            // OP is the hash join reading destination node property. Continue push limit down.
            op = op->getChild(0).get();
        }
        if (op->getChild(0)->getOperatorType() == LogicalOperatorType::PATH_PROPERTY_PROBE) {
            // LCOV_EXCL_START
            if (op->getChild(0)->getChild(0)->getOperatorType() != LogicalOperatorType::GDS_CALL) {
                throw RuntimeException(
                    "Trying to push limit to a non-GDS operator. This should never happen.");
            }
            // LCOV_EXCL_STOP
            auto& gds = op->getChild(0)->getChild(0)->cast<LogicalGDSCall>();
            gds.setLimitNum(skipNumber + limitNumber);
        }
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
