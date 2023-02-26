#include "optimizer/projection_push_down_optimizer.h"

#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

void ProjectionPushDownOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void ProjectionPushDownOptimizer::visitOperator(LogicalOperator* op) {
    switch (op->getOperatorType()) {
    case LogicalOperatorType::ACCUMULATE: {
        visitAccumulate(op);
    } break;
    case LogicalOperatorType::FILTER: {
        visitFilter(op);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        visitHashJoin(op);
    } break;
    case LogicalOperatorType::PROJECTION: {
        visitProjection(op);
        return;
    }
    case LogicalOperatorType::INTERSECT: {
        visitIntersect(op);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        visitOrderBy(op);
    } break;
    default:
        break;
    }
    for (auto i = 0; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
}

void ProjectionPushDownOptimizer::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    for (auto& expression : accumulate->getExpressions()) {
        collectPropertiesInUse(expression);
    }
}

void ProjectionPushDownOptimizer::visitFilter(planner::LogicalOperator* op) {
    auto filter = (LogicalFilter*)op;
    collectPropertiesInUse(filter->getPredicate());
}

void ProjectionPushDownOptimizer::visitHashJoin(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    for (auto& joinNodeID : hashJoin->getJoinNodeIDs()) {
        collectPropertiesInUse(joinNodeID);
    }
    if (hashJoin->getJoinType() == JoinType::MARK) { // no need to perform push down for mark join.
        return;
    }
    auto expressionsBeforePruning = hashJoin->getExpressionsToMaterialize();
    expression_set expressionsAfterPruning;
    for (auto& expression : expressionsBeforePruning) {
        if (expression->expressionType != common::PROPERTY ||
            propertiesInUse.contains(expression)) {
            expressionsAfterPruning.insert(expression);
        }
    }
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        // TODO(Xiyang): replace this with a separate optimizer.
        return;
    }
    hashJoin->setExpressionsToMaterialize(expressionsAfterPruning);
    auto projectionExpressions =
        expression_vector{expressionsAfterPruning.begin(), expressionsAfterPruning.end()};
    auto projection = std::make_shared<LogicalProjection>(
        std::move(projectionExpressions), hashJoin->getChild(1));
    hashJoin->setChild(1, std::move(projection));
}

void ProjectionPushDownOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    collectPropertiesInUse(intersect->getIntersectNodeID());
    for (auto i = 0u; i < intersect->getNumBuilds(); ++i) {
        auto buildInfo = intersect->getBuildInfo(i);
        collectPropertiesInUse(buildInfo->keyNodeID);
        for (auto& expression : buildInfo->expressionsToMaterialize) {
            collectPropertiesInUse(expression);
        }
    }
}

void ProjectionPushDownOptimizer::visitProjection(LogicalOperator* op) {
    // Projection operator defines the start of a projection push down until the next projection
    // operator is seen.
    ProjectionPushDownOptimizer optimizer;
    auto projection = (LogicalProjection*)op;
    for (auto& expression : projection->getExpressionsToProject()) {
        optimizer.collectPropertiesInUse(expression);
    }
    optimizer.visitOperator(op->getChild(0).get());
}

void ProjectionPushDownOptimizer::visitOrderBy(planner::LogicalOperator* op) {
    auto orderBy = (LogicalOrderBy*)op;
    for (auto& expression : orderBy->getExpressionsToOrderBy()) {
        collectPropertiesInUse(expression);
    }
    for (auto& expression : orderBy->getExpressionsToMaterialize()) {
        collectPropertiesInUse(expression);
    }
}

void ProjectionPushDownOptimizer::collectPropertiesInUse(
    std::shared_ptr<binder::Expression> expression) {
    if (expression->expressionType == common::PROPERTY) {
        propertiesInUse.insert(std::move(expression));
        return;
    }
    for (auto& child : expression->getChildren()) {
        collectPropertiesInUse(child);
    }
}

} // namespace optimizer
} // namespace kuzu
