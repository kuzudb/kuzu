#include "optimizer/projection_push_down_optimizer.h"

#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "planner/logical_plan/logical_operator/logical_unwind.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

void ProjectionPushDownOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void ProjectionPushDownOptimizer::visitOperator(LogicalOperator* op) {
    visitOperatorSwitch(op);
    if (op->getOperatorType() == LogicalOperatorType::PROJECTION) {
        // We will start a new optimizer once a projection is encountered.
        return;
    }
    // top-down traversal
    for (auto i = 0; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
}

void ProjectionPushDownOptimizer::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    auto expressionsBeforePruning = accumulate->getExpressions();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    accumulate->setExpressions(expressionsAfterPruning);
    auto projection = std::make_shared<LogicalProjection>(
        std::move(expressionsAfterPruning), accumulate->getChild(0));
    accumulate->setChild(0, std::move(projection));
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
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        // TODO(Xiyang): replace this with a separate optimizer.
        return;
    }
    hashJoin->setExpressionsToMaterialize(expressionsAfterPruning);
    auto projection = std::make_shared<LogicalProjection>(
        std::move(expressionsAfterPruning), hashJoin->getChild(1));
    hashJoin->setChild(1, std::move(projection));
}

void ProjectionPushDownOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    collectPropertiesInUse(intersect->getIntersectNodeID());
    for (auto i = 0u; i < intersect->getNumBuilds(); ++i) {
        auto buildInfo = intersect->getBuildInfo(i);
        collectPropertiesInUse(buildInfo->keyNodeID);
        // Note: we have a potential bug under intersect.cpp. The following code ensures build key
        // and intersect key always appear as the first and second column. Should be removed once
        // the bug is fixed.
        expression_vector expressionsBeforePruning;
        expression_vector expressionsAfterPruning;
        for (auto& expression : buildInfo->expressionsToMaterialize) {
            if (expression->getUniqueName() == intersect->getIntersectNodeID()->getUniqueName() ||
                expression->getUniqueName() == buildInfo->keyNodeID->getUniqueName()) {
                continue;
            }
            expressionsBeforePruning.push_back(expression);
        }
        expressionsAfterPruning.push_back(buildInfo->keyNodeID);
        expressionsAfterPruning.push_back(intersect->getIntersectNodeID());
        for (auto& expression : pruneExpressions(expressionsBeforePruning)) {
            expressionsAfterPruning.push_back(expression);
        }
        if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
            return;
        }
        buildInfo->setExpressionsToMaterialize(expressionsAfterPruning);
        auto childIdx = i + 1; // skip probe
        auto projection = std::make_shared<LogicalProjection>(
            std::move(expressionsAfterPruning), intersect->getChild(childIdx));
        intersect->setChild(childIdx, std::move(projection));
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
    auto expressionsBeforePruning = orderBy->getExpressionsToMaterialize();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    orderBy->setExpressionsToMaterialize(expressionsAfterPruning);
    auto projection = std::make_shared<LogicalProjection>(
        std::move(expressionsAfterPruning), orderBy->getChild(0));
    orderBy->setChild(0, std::move(projection));
}

void ProjectionPushDownOptimizer::visitUnwind(planner::LogicalOperator* op) {
    auto unwind = (LogicalUnwind*)op;
    collectPropertiesInUse(unwind->getExpression());
}

void ProjectionPushDownOptimizer::visitCreateNode(planner::LogicalOperator* op) {
    auto createNode = (LogicalCreateNode*)op;
    for (auto i = 0u; i < createNode->getNumNodes(); ++i) {
        collectPropertiesInUse(createNode->getPrimaryKey(i));
    }
}

void ProjectionPushDownOptimizer::visitCreateRel(planner::LogicalOperator* op) {
    auto createRel = (LogicalCreateRel*)op;
    for (auto i = 0; i < createRel->getNumRels(); ++i) {
        auto rel = createRel->getRel(i);
        collectPropertiesInUse(rel->getSrcNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getDstNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getInternalIDProperty());
        for (auto& setItem : createRel->getSetItems(i)) {
            collectPropertiesInUse(setItem.second);
        }
    }
}

void ProjectionPushDownOptimizer::visitDeleteNode(planner::LogicalOperator* op) {
    auto deleteNode = (LogicalDeleteNode*)op;
    for (auto i = 0u; i < deleteNode->getNumNodes(); ++i) {
        collectPropertiesInUse(deleteNode->getNode(i)->getInternalIDProperty());
        collectPropertiesInUse(deleteNode->getPrimaryKey(i));
    }
}

void ProjectionPushDownOptimizer::visitDeleteRel(planner::LogicalOperator* op) {
    auto deleteRel = (LogicalDeleteRel*)op;
    for (auto i = 0; i < deleteRel->getNumRels(); ++i) {
        auto rel = deleteRel->getRel(i);
        collectPropertiesInUse(rel->getSrcNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getDstNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getInternalIDProperty());
    }
}

void ProjectionPushDownOptimizer::visitSetNodeProperty(planner::LogicalOperator* op) {
    auto setNodeProperty = (LogicalSetNodeProperty*)op;
    for (auto i = 0u; i < setNodeProperty->getNumNodes(); ++i) {
        collectPropertiesInUse(setNodeProperty->getNode(i)->getInternalIDProperty());
        collectPropertiesInUse(setNodeProperty->getSetItem(i).second);
    }
}

void ProjectionPushDownOptimizer::visitSetRelProperty(planner::LogicalOperator* op) {
    auto setRelProperty = (LogicalSetRelProperty*)op;
    for (auto i = 0; i < setRelProperty->getNumRels(); ++i) {
        auto rel = setRelProperty->getRel(i);
        collectPropertiesInUse(rel->getSrcNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getDstNode()->getInternalIDProperty());
        collectPropertiesInUse(rel->getInternalIDProperty());
        collectPropertiesInUse(setRelProperty->getSetItem(i).second);
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

binder::expression_vector ProjectionPushDownOptimizer::pruneExpressions(
    const binder::expression_vector& expressions) {
    expression_set expressionsAfterPruning;
    for (auto& expression : expressions) {
        if (expression->expressionType != common::PROPERTY ||
            propertiesInUse.contains(expression)) {
            expressionsAfterPruning.insert(expression);
        }
    }
    return expression_vector{expressionsAfterPruning.begin(), expressionsAfterPruning.end()};
}

} // namespace optimizer
} // namespace kuzu
