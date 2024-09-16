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
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/logical_unwind.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/operator/persistent/logical_set.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

void ProjectionPushDownOptimizer::rewrite(LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void ProjectionPushDownOptimizer::visitOperator(LogicalOperator* op) {
    visitOperatorSwitch(op);
    if (op->getOperatorType() == LogicalOperatorType::PROJECTION) {
        // We will start a new optimizer once a projection is encountered.
        return;
    }
    // top-down traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    op->computeFlatSchema();
}

void ProjectionPushDownOptimizer::visitPathPropertyProbe(LogicalOperator* op) {
    auto& pathPropertyProbe = op->cast<LogicalPathPropertyProbe>();
    KU_ASSERT(
        pathPropertyProbe.getChild(0)->getOperatorType() == LogicalOperatorType::RECURSIVE_EXTEND);
    auto& recursiveExtend = pathPropertyProbe.getChild(0)->cast<LogicalRecursiveExtend>();
    auto boundNodeID = recursiveExtend.getBoundNode()->getInternalID();
    collectExpressionsInUse(boundNodeID);
    auto rel = recursiveExtend.getRel();
    if (!nodeOrRelInUse.contains(rel)) {
        pathPropertyProbe.setJoinType(RecursiveJoinType::TRACK_NONE);
        recursiveExtend.setJoinType(RecursiveJoinType::TRACK_NONE);
    }
}

void ProjectionPushDownOptimizer::visitExtend(LogicalOperator* op) {
    auto& extend = op->constCast<LogicalExtend>();
    auto boundNodeID = extend.getBoundNode()->getInternalID();
    collectExpressionsInUse(boundNodeID);
}

void ProjectionPushDownOptimizer::visitAccumulate(LogicalOperator* op) {
    auto& accumulate = op->constCast<LogicalAccumulate>();
    if (accumulate.getAccumulateType() != AccumulateType::REGULAR) {
        return;
    }
    auto expressionsBeforePruning = accumulate.getPayloads();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitFilter(LogicalOperator* op) {
    auto& filter = op->constCast<LogicalFilter>();
    collectExpressionsInUse(filter.getPredicate());
}

void ProjectionPushDownOptimizer::visitHashJoin(LogicalOperator* op) {
    auto& hashJoin = op->constCast<LogicalHashJoin>();
    for (auto& [probeJoinKey, buildJoinKey] : hashJoin.getJoinConditions()) {
        collectExpressionsInUse(probeJoinKey);
        collectExpressionsInUse(buildJoinKey);
    }
    if (hashJoin.getJoinType() == JoinType::MARK) { // no need to perform push down for mark join.
        return;
    }
    auto expressionsBeforePruning = hashJoin.getExpressionsToMaterialize();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        // TODO(Xiyang): replace this with a separate optimizer.
        return;
    }
    preAppendProjection(op, 1, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitIntersect(LogicalOperator* op) {
    auto& intersect = op->constCast<LogicalIntersect>();
    collectExpressionsInUse(intersect.getIntersectNodeID());
    for (auto i = 0u; i < intersect.getNumBuilds(); ++i) {
        auto childIdx = i + 1; // skip probe
        auto keyNodeID = intersect.getKeyNodeID(i);
        collectExpressionsInUse(keyNodeID);
        // Note: we have a potential bug under intersect.cpp. The following code ensures build key
        // and intersect key always appear as the first and second column. Should be removed once
        // the bug is fixed.
        expression_vector expressionsBeforePruning;
        expression_vector expressionsAfterPruning;
        for (auto& expression :
            intersect.getChild(childIdx)->getSchema()->getExpressionsInScope()) {
            if (expression->getUniqueName() == intersect.getIntersectNodeID()->getUniqueName() ||
                expression->getUniqueName() == keyNodeID->getUniqueName()) {
                continue;
            }
            expressionsBeforePruning.push_back(expression);
        }
        expressionsAfterPruning.push_back(keyNodeID);
        expressionsAfterPruning.push_back(intersect.getIntersectNodeID());
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
    auto& projection = op->constCast<LogicalProjection>();
    for (auto& expression : projection.getExpressionsToProject()) {
        optimizer.collectExpressionsInUse(expression);
    }
    optimizer.visitOperator(op->getChild(0).get());
}

void ProjectionPushDownOptimizer::visitOrderBy(LogicalOperator* op) {
    auto& orderBy = op->constCast<LogicalOrderBy>();
    for (auto& expression : orderBy.getExpressionsToOrderBy()) {
        collectExpressionsInUse(expression);
    }
    auto expressionsBeforePruning = orderBy.getChild(0)->getSchema()->getExpressionsInScope();
    auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
    if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
        return;
    }
    preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitUnwind(LogicalOperator* op) {
    auto& unwind = op->constCast<LogicalUnwind>();
    collectExpressionsInUse(unwind.getInExpr());
}

void ProjectionPushDownOptimizer::visitInsert(LogicalOperator* op) {
    auto& insert = op->constCast<LogicalInsert>();
    for (auto& info : insert.getInfos()) {
        visitInsertInfo(info);
    }
}

void ProjectionPushDownOptimizer::visitDelete(LogicalOperator* op) {
    auto& delete_ = op->constCast<LogicalDelete>();
    auto& infos = delete_.getInfos();
    KU_ASSERT(!infos.empty());
    switch (infos[0].tableType) {
    case TableType::NODE: {
        for (auto& info : infos) {
            auto& node = info.pattern->constCast<NodeExpression>();
            collectExpressionsInUse(node.getInternalID());
            for (auto entry : node.getEntries()) {
                collectExpressionsInUse(node.getPrimaryKey(entry->getTableID()));
            }
        }
    } break;
    case TableType::REL: {
        for (auto& info : infos) {
            auto& rel = info.pattern->constCast<RelExpression>();
            collectExpressionsInUse(rel.getSrcNode()->getInternalID());
            collectExpressionsInUse(rel.getDstNode()->getInternalID());
            KU_ASSERT(rel.getRelType() == QueryRelType::NON_RECURSIVE);
            if (!rel.isEmpty()) {
                collectExpressionsInUse(rel.getInternalIDProperty());
            }
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void ProjectionPushDownOptimizer::visitMerge(LogicalOperator* op) {
    auto& merge = op->constCast<LogicalMerge>();
    collectExpressionsInUse(merge.getExistenceMark());
    for (auto& info : merge.getInsertNodeInfos()) {
        visitInsertInfo(info);
    }
    for (auto& info : merge.getInsertRelInfos()) {
        visitInsertInfo(info);
    }
    for (auto& info : merge.getOnCreateSetNodeInfos()) {
        visitSetInfo(info);
    }
    for (auto& info : merge.getOnMatchSetNodeInfos()) {
        visitSetInfo(info);
    }
    for (auto& info : merge.getOnCreateSetRelInfos()) {
        visitSetInfo(info);
    }
    for (auto& info : merge.getOnMatchSetRelInfos()) {
        visitSetInfo(info);
    }
}

void ProjectionPushDownOptimizer::visitSetProperty(LogicalOperator* op) {
    auto& set = op->constCast<LogicalSetProperty>();
    for (auto& info : set.getInfos()) {
        visitSetInfo(info);
    }
}

void ProjectionPushDownOptimizer::visitCopyFrom(LogicalOperator* op) {
    auto& copyFrom = op->constCast<LogicalCopyFrom>();
    for (auto& expr : copyFrom.getInfo()->source->getColumns()) {
        collectExpressionsInUse(expr);
    }
    collectExpressionsInUse(copyFrom.getInfo()->offset);
}

void ProjectionPushDownOptimizer::visitTableFunctionCall(LogicalOperator* op) {
    auto& tableFunctionCall = op->cast<LogicalTableFunctionCall>();
    std::vector<bool> columnSkips;
    for (auto& column : tableFunctionCall.getColumns()) {
        columnSkips.push_back(!variablesInUse.contains(column));
    }
    tableFunctionCall.setColumnSkips(std::move(columnSkips));
}

void ProjectionPushDownOptimizer::visitSetInfo(const binder::BoundSetPropertyInfo& info) {
    switch (info.tableType) {
    case TableType::NODE: {
        auto& node = info.pattern->constCast<NodeExpression>();
        collectExpressionsInUse(node.getInternalID());
        if (info.updatePk) {
            collectExpressionsInUse(info.column);
        }
    } break;
    case TableType::REL: {
        auto& rel = info.pattern->constCast<RelExpression>();
        collectExpressionsInUse(rel.getSrcNode()->getInternalID());
        collectExpressionsInUse(rel.getDstNode()->getInternalID());
        collectExpressionsInUse(rel.getInternalIDProperty());
    } break;
    default:
        KU_UNREACHABLE;
    }
    collectExpressionsInUse(info.columnData);
}

void ProjectionPushDownOptimizer::visitInsertInfo(const LogicalInsertInfo& info) {
    if (info.tableType == common::TableType::REL) {
        auto& rel = info.pattern->constCast<RelExpression>();
        collectExpressionsInUse(rel.getSrcNode()->getInternalID());
        collectExpressionsInUse(rel.getDstNode()->getInternalID());
        collectExpressionsInUse(rel.getInternalIDProperty());
    }
    for (auto i = 0u; i < info.columnExprs.size(); ++i) {
        if (info.isReturnColumnExprs[i]) {
            collectExpressionsInUse(info.columnExprs[i]);
        }
        collectExpressionsInUse(info.columnDataExprs[i]);
    }
}

// See comments above this class for how to collect expressions in use.
void ProjectionPushDownOptimizer::collectExpressionsInUse(
    std::shared_ptr<binder::Expression> expression) {
    switch (expression->expressionType) {
    case ExpressionType::PROPERTY: {
        propertiesInUse.insert(expression);
        return;
    }
    case ExpressionType::VARIABLE: {
        variablesInUse.insert(expression);
        return;
    }
    case ExpressionType::PATTERN: {
        nodeOrRelInUse.insert(expression);
        for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
            collectExpressionsInUse(child);
        }
        return;
    }
    default:
        for (auto& child : ExpressionChildrenCollector::collectChildren(*expression)) {
            collectExpressionsInUse(child);
        }
    }
}

binder::expression_vector ProjectionPushDownOptimizer::pruneExpressions(
    const binder::expression_vector& expressions) {
    expression_set expressionsAfterPruning;
    for (auto& expression : expressions) {
        switch (expression->expressionType) {
        case ExpressionType::PROPERTY: {
            if (propertiesInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        case ExpressionType::VARIABLE: {
            if (variablesInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        case ExpressionType::PATTERN: {
            if (nodeOrRelInUse.contains(expression)) {
                expressionsAfterPruning.insert(expression);
            }
        } break;
        default: // We don't track other expression types so always assume they will be in use.
            expressionsAfterPruning.insert(expression);
        }
    }
    return expression_vector{expressionsAfterPruning.begin(), expressionsAfterPruning.end()};
}

void ProjectionPushDownOptimizer::preAppendProjection(LogicalOperator* op, idx_t childIdx,
    binder::expression_vector expressions) {
    if (expressions.empty()) {
        // We don't have a way to handle
        return;
    }
    auto projection =
        std::make_shared<LogicalProjection>(std::move(expressions), op->getChild(childIdx));
    projection->computeFlatSchema();
    op->setChild(childIdx, std::move(projection));
}

} // namespace optimizer
} // namespace kuzu
