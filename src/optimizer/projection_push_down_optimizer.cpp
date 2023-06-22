#include "optimizer/projection_push_down_optimizer.h"

#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
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
    op->computeFlatSchema();
}

void ProjectionPushDownOptimizer::visitPathPropertyProbe(planner::LogicalOperator* op) {
    auto pathPropertyProbe = (LogicalPathPropertyProbe*)op;
    assert(
        pathPropertyProbe->getChild(0)->getOperatorType() == LogicalOperatorType::RECURSIVE_EXTEND);
    auto recursiveExtend = (LogicalRecursiveExtend*)pathPropertyProbe->getChild(0).get();
    auto boundNodeID = recursiveExtend->getBoundNode()->getInternalIDProperty();
    collectExpressionsInUse(boundNodeID);
    auto rel = recursiveExtend->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    if (!variablesInUse.contains(rel)) {
        recursiveExtend->setJoinType(planner::RecursiveJoinType::TRACK_NONE);
        // TODO(Xiyang): we should remove pathPropertyProbe if we don't need to track path
        pathPropertyProbe->setChildren(
            std::vector<std::shared_ptr<LogicalOperator>>{pathPropertyProbe->getChild(0)});
    } else {
        // Pre-append projection to rel property build.
        expression_vector properties;
        for (auto& expression : recursiveInfo->rel->getPropertyExpressions()) {
            properties.push_back(expression->copy());
        }
        preAppendProjection(op, 2, properties);
    }
}

void ProjectionPushDownOptimizer::visitExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalExtend*)op;
    auto boundNodeID = extend->getBoundNode()->getInternalIDProperty();
    collectExpressionsInUse(boundNodeID);
}

void ProjectionPushDownOptimizer::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    auto expressionsBeforePruning = accumulate->getExpressions();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitFilter(planner::LogicalOperator* op) {
    auto filter = (LogicalFilter*)op;
    collectExpressionsInUse(filter->getPredicate());
}

void ProjectionPushDownOptimizer::visitHashJoin(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    for (auto& joinNodeID : hashJoin->getJoinNodeIDs()) {
        collectExpressionsInUse(joinNodeID);
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
    preAppendProjection(op, 1, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    collectExpressionsInUse(intersect->getIntersectNodeID());
    for (auto i = 0u; i < intersect->getNumBuilds(); ++i) {
        auto childIdx = i + 1; // skip probe
        auto keyNodeID = intersect->getKeyNodeID(i);
        collectExpressionsInUse(keyNodeID);
        // Note: we have a potential bug under intersect.cpp. The following code ensures build key
        // and intersect key always appear as the first and second column. Should be removed once
        // the bug is fixed.
        expression_vector expressionsBeforePruning;
        expression_vector expressionsAfterPruning;
        for (auto& expression :
            intersect->getChild(childIdx)->getSchema()->getExpressionsInScope()) {
            if (expression->getUniqueName() == intersect->getIntersectNodeID()->getUniqueName() ||
                expression->getUniqueName() == keyNodeID->getUniqueName()) {
                continue;
            }
            expressionsBeforePruning.push_back(expression);
        }
        expressionsAfterPruning.push_back(keyNodeID);
        expressionsAfterPruning.push_back(intersect->getIntersectNodeID());
        for (auto& expression : pruneExpressions(expressionsBeforePruning)) {
            expressionsAfterPruning.push_back(expression);
        }
        if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
            return;
        }

        preAppendProjection(op, childIdx, expressionsAfterPruning);
    }
}

void ProjectionPushDownOptimizer::visitProjection(LogicalOperator* op) {
    // Projection operator defines the start of a projection push down until the next projection
    // operator is seen.
    ProjectionPushDownOptimizer optimizer;
    auto projection = (LogicalProjection*)op;
    for (auto& expression : projection->getExpressionsToProject()) {
        optimizer.collectExpressionsInUse(expression);
    }
    optimizer.visitOperator(op->getChild(0).get());
}

void ProjectionPushDownOptimizer::visitOrderBy(planner::LogicalOperator* op) {
    auto orderBy = (LogicalOrderBy*)op;
    for (auto& expression : orderBy->getExpressionsToOrderBy()) {
        collectExpressionsInUse(expression);
    }
    auto expressionsBeforePruning = orderBy->getExpressionsToMaterialize();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitUnwind(planner::LogicalOperator* op) {
    auto unwind = (LogicalUnwind*)op;
    collectExpressionsInUse(unwind->getExpression());
}

void ProjectionPushDownOptimizer::visitCreateNode(planner::LogicalOperator* op) {
    auto createNode = (LogicalCreateNode*)op;
    for (auto i = 0u; i < createNode->getNumNodes(); ++i) {
        auto primaryKey = createNode->getPrimaryKey(i);
        if (primaryKey != nullptr) {
            collectExpressionsInUse(createNode->getPrimaryKey(i));
        }
    }
}

void ProjectionPushDownOptimizer::visitCreateRel(planner::LogicalOperator* op) {
    auto createRel = (LogicalCreateRel*)op;
    for (auto i = 0; i < createRel->getNumRels(); ++i) {
        auto rel = createRel->getRel(i);
        collectExpressionsInUse(rel->getSrcNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getDstNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getInternalIDProperty());
        for (auto& setItem : createRel->getSetItems(i)) {
            collectExpressionsInUse(setItem.second);
        }
    }
}

void ProjectionPushDownOptimizer::visitDeleteNode(planner::LogicalOperator* op) {
    auto deleteNode = (LogicalDeleteNode*)op;
    for (auto i = 0u; i < deleteNode->getNumNodes(); ++i) {
        collectExpressionsInUse(deleteNode->getNode(i)->getInternalIDProperty());
        collectExpressionsInUse(deleteNode->getPrimaryKey(i));
    }
}

void ProjectionPushDownOptimizer::visitDeleteRel(planner::LogicalOperator* op) {
    auto deleteRel = (LogicalDeleteRel*)op;
    for (auto i = 0; i < deleteRel->getNumRels(); ++i) {
        auto rel = deleteRel->getRel(i);
        collectExpressionsInUse(rel->getSrcNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getDstNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getInternalIDProperty());
    }
}

void ProjectionPushDownOptimizer::visitSetNodeProperty(planner::LogicalOperator* op) {
    auto setNodeProperty = (LogicalSetNodeProperty*)op;
    for (auto i = 0u; i < setNodeProperty->getNumNodes(); ++i) {
        collectExpressionsInUse(setNodeProperty->getNode(i)->getInternalIDProperty());
        collectExpressionsInUse(setNodeProperty->getSetItem(i).second);
    }
}

void ProjectionPushDownOptimizer::visitSetRelProperty(planner::LogicalOperator* op) {
    auto setRelProperty = (LogicalSetRelProperty*)op;
    for (auto i = 0; i < setRelProperty->getNumRels(); ++i) {
        auto rel = setRelProperty->getRel(i);
        collectExpressionsInUse(rel->getSrcNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getDstNode()->getInternalIDProperty());
        collectExpressionsInUse(rel->getInternalIDProperty());
        collectExpressionsInUse(setRelProperty->getSetItem(i).second);
    }
}

// See comments above this class for how to collect expressions in use.
void ProjectionPushDownOptimizer::collectExpressionsInUse(
    std::shared_ptr<binder::Expression> expression) {
    if (expression->expressionType == common::VARIABLE) {
        variablesInUse.insert(std::move(expression));
        return;
    }
    if (expression->expressionType == common::PROPERTY) {
        propertiesInUse.insert(std::move(expression));
        return;
    }
    for (auto& child : expression->getChildren()) {
        collectExpressionsInUse(child);
    }
}

binder::expression_vector ProjectionPushDownOptimizer::pruneExpressions(
    const binder::expression_vector& expressions) {
    expression_set expressionsAfterPruning;
    for (auto& expression : expressions) {
        switch (expression->expressionType) {
        case common::VARIABLE: {
            if (variablesInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        case common::PROPERTY: {
            if (propertiesInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        default: // We don't track other expression types so always assume they will be in use.
            expressionsAfterPruning.insert(expression);
        }
    }
    return expression_vector{expressionsAfterPruning.begin(), expressionsAfterPruning.end()};
}

void ProjectionPushDownOptimizer::preAppendProjection(
    planner::LogicalOperator* op, uint32_t childIdx, binder::expression_vector expressions) {
    auto projection =
        std::make_shared<LogicalProjection>(std::move(expressions), op->getChild(childIdx));
    projection->computeFlatSchema();
    op->setChild(childIdx, std::move(projection));
}

} // namespace optimizer
} // namespace kuzu
