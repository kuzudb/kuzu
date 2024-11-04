#include "optimizer/limit_push_down_optimizer.h"

#include "planner/operator/logical_distinct.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace optimizer {

void LimitPushDownOptimizer::rewrite(LogicalPlan* plan) {
    plan->setLastOperator(visitOperator(plan->getLastOperator()));
}

static LogicalScanNodeTable* isSimpleScanNode(const std::shared_ptr<LogicalOperator>& op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::SCAN_NODE_TABLE: {
        return &op->cast<LogicalScanNodeTable>();
    }
    case LogicalOperatorType::PROJECTION: {
        return isSimpleScanNode(op->getChild(0));
    }
    default:
        return nullptr;
    }
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
    case LogicalOperatorType::HASH_JOIN: {
        if (limitOperator == nullptr) {
            return op;
        }
        auto& hashJoin = op->cast<LogicalHashJoin>();
        // Push down limit to probe side
        if (hashJoin.getJoinType() == JoinType::LEFT) {
            // Add a limit to the probe side,and keep the original limit as well.
            auto optimizer = LimitPushDownOptimizer(context);
            optimizer.limitOperator = limitOperator;
            auto newChild = optimizer.visitOperator(op->getChild(0));
            op->setChild(0, newChild);
        } else if (hashJoin.getJoinType() == JoinType::INNER) {
            LogicalScanNodeTable* scanNode = isSimpleScanNode(hashJoin.getChild(0));
            // If probe side is ScanNode and build side has semiMasker, we can push down limit to
            // build side
            if (scanNode) {
                const auto& ops = findLogicalSemiMasker(hashJoin.getChild(1))->getTargetOperators();
                if (std::find(ops.begin(), ops.end(), scanNode) != ops.end()) {
                    auto optimizer = LimitPushDownOptimizer(context);
                    optimizer.limitOperator = limitOperator;
                    auto newChild = optimizer.visitOperator(op->getChild(1));
                    op->setChild(1, newChild);
                    limitOperator = nullptr;
                    return op;
                }
            }
            scanNode = isSimpleScanNode(hashJoin.getChild(1));
            // If build side is ScanNode ,we can push down limit to probe side
            if (scanNode) {
                auto optimizer = LimitPushDownOptimizer(context);
                optimizer.limitOperator = limitOperator;
                auto newChild = optimizer.visitOperator(op->getChild(0));
                op->setChild(0, newChild);
                limitOperator = nullptr;
                return op;
            }
        } else {
            // subquery
        }
        return finishPushDown(op);
    }
    default: {
        return finishPushDown(op);
    }
    }
}

} // namespace optimizer
} // namespace kuzu
