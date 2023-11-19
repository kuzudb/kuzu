#include "optimizer/projection_push_down_optimizer.h"

#include "binder/expression_visitor.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_accumulate.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_intersect.h"
#include "planner/operator/logical_order_by.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/logical_unwind.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/operator/persistent/logical_set.h"

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
    KU_ASSERT(
        pathPropertyProbe->getChild(0)->getOperatorType() == LogicalOperatorType::RECURSIVE_EXTEND);
    auto recursiveExtend = (LogicalRecursiveExtend*)pathPropertyProbe->getChild(0).get();
    auto boundNodeID = recursiveExtend->getBoundNode()->getInternalID();
    collectExpressionsInUse(boundNodeID);
    auto rel = recursiveExtend->getRel();
    if (!patternInUse.contains(rel)) {
        pathPropertyProbe->setJoinType(planner::RecursiveJoinType::TRACK_NONE);
        recursiveExtend->setJoinType(planner::RecursiveJoinType::TRACK_NONE);
    }
}

void ProjectionPushDownOptimizer::visitExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalExtend*)op;
    auto boundNodeID = extend->getBoundNode()->getInternalID();
    collectExpressionsInUse(boundNodeID);
}

void ProjectionPushDownOptimizer::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    if (accumulate->getAccumulateType() != AccumulateType::REGULAR) {
        return;
    }
    auto expressionsBeforePruning = accumulate->getExpressionsToAccumulate();
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
    for (auto& [probeJoinKey, buildJoinKey] : hashJoin->getJoinConditions()) {
        collectExpressionsInUse(probeJoinKey);
        collectExpressionsInUse(buildJoinKey);
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
    auto expressionsBeforePruning = orderBy->getChild(0)->getSchema()->getExpressionsInScope();
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

void ProjectionPushDownOptimizer::visitInsertNode(planner::LogicalOperator* op) {
    auto insertNode = (LogicalInsertNode*)op;
    for (auto& info : insertNode->getInfosRef()) {
        for (auto& setItem : info->setItems) {
            collectExpressionsInUse(setItem.second);
        }
    }
}

void ProjectionPushDownOptimizer::visitInsertRel(planner::LogicalOperator* op) {
    auto insertRel = (LogicalInsertRel*)op;
    for (auto& info : insertRel->getInfosRef()) {
        auto rel = info->rel;
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
        for (auto& setItem : info->setItems) {
            collectExpressionsInUse(setItem.second);
        }
    }
}

void ProjectionPushDownOptimizer::visitDeleteNode(planner::LogicalOperator* op) {
    auto deleteNode = (LogicalDeleteNode*)op;
    for (auto info : deleteNode->getInfos()) {
        collectExpressionsInUse(info->node->getInternalID());
    }
}

void ProjectionPushDownOptimizer::visitDeleteRel(planner::LogicalOperator* op) {
    auto deleteRel = (LogicalDeleteRel*)op;
    for (auto& rel : deleteRel->getRelsRef()) {
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
    }
}

// TODO(Xiyang): come back and refactor this after changing insert interface
void ProjectionPushDownOptimizer::visitMerge(planner::LogicalOperator* op) {
    auto merge = (LogicalMerge*)op;
    collectExpressionsInUse(merge->getMark());
    for (auto& info : merge->getInsertNodeInfosRef()) {
        for (auto& setItem : info->setItems) {
            collectExpressionsInUse(setItem.second);
        }
    }
    for (auto& info : merge->getInsertRelInfosRef()) {
        auto rel = info->rel;
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
        for (auto& setItem : info->setItems) {
            collectExpressionsInUse(setItem.second);
        }
    }
    for (auto& info : merge->getOnCreateSetNodeInfosRef()) {
        auto node = (NodeExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(node->getInternalID());
        collectExpressionsInUse(info->setItem.second);
    }
    for (auto& info : merge->getOnMatchSetNodeInfosRef()) {
        auto node = (NodeExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(node->getInternalID());
        collectExpressionsInUse(info->setItem.second);
    }
    for (auto& info : merge->getOnCreateSetRelInfosRef()) {
        auto rel = (RelExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
        collectExpressionsInUse(info->setItem.second);
    }
    for (auto& info : merge->getOnMatchSetRelInfosRef()) {
        auto rel = (RelExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
        collectExpressionsInUse(info->setItem.second);
    }
}

void ProjectionPushDownOptimizer::visitSetNodeProperty(planner::LogicalOperator* op) {
    auto setNodeProperty = (LogicalSetNodeProperty*)op;
    for (auto& info : setNodeProperty->getInfosRef()) {
        auto node = (NodeExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(node->getInternalID());
        collectExpressionsInUse(info->setItem.second);
    }
}

void ProjectionPushDownOptimizer::visitSetRelProperty(planner::LogicalOperator* op) {
    auto setRelProperty = (LogicalSetRelProperty*)op;
    for (auto& info : setRelProperty->getInfosRef()) {
        auto rel = (RelExpression*)info->nodeOrRel.get();
        collectExpressionsInUse(rel->getSrcNode()->getInternalID());
        collectExpressionsInUse(rel->getDstNode()->getInternalID());
        collectExpressionsInUse(rel->getInternalIDProperty());
        collectExpressionsInUse(info->setItem.second);
    }
}

// See comments above this class for how to collect expressions in use.
void ProjectionPushDownOptimizer::collectExpressionsInUse(
    std::shared_ptr<binder::Expression> expression) {
    if (expression->expressionType == ExpressionType::PROPERTY) {
        propertiesInUse.insert(std::move(expression));
        return;
    }
    if (expression->expressionType == ExpressionType::PATTERN) {
        patternInUse.insert(expression);
    }
    for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
        collectExpressionsInUse(child);
    }
}

binder::expression_vector ProjectionPushDownOptimizer::pruneExpressions(
    const binder::expression_vector& expressions) {
    expression_set expressionsAfterPruning;
    for (auto& expression : expressions) {
        switch (expression->expressionType) {
        case ExpressionType::PATTERN: {
            if (patternInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        case ExpressionType::PROPERTY: {
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
