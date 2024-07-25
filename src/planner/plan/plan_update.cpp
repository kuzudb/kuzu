#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::planUpdatingClause(const BoundUpdatingClause* updatingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planUpdatingClause(updatingClause, *plan);
    }
}

void Planner::planUpdatingClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    switch (updatingClause->getClauseType()) {
    case ClauseType::INSERT: {
        planInsertClause(updatingClause, plan);
        return;
    }
    case ClauseType::MERGE: {
        planMergeClause(updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        planSetClause(updatingClause, plan);
        return;
    }
    case ClauseType::DELETE_: {
        planDeleteClause(updatingClause, plan);
        return;
    }
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planInsertClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    auto& insertClause = updatingClause->constCast<BoundInsertClause>();
    if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
        appendDummyScan(plan);
    } else {
        appendAccumulate(plan);
    }
    if (insertClause.hasNodeInfo()) {
        appendInsertNode(insertClause.getNodeInfos(), plan);
    }
    if (insertClause.hasRelInfo()) {
        appendInsertRel(insertClause.getRelInfos(), plan);
    }
}

void Planner::planMergeClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    auto& mergeClause = updatingClause->constCast<BoundMergeClause>();
    binder::expression_vector predicates;
    if (mergeClause.hasPredicate()) {
        predicates = mergeClause.getPredicate()->splitOnAND();
    }
    expression_vector corrExprs;
    if (!plan.isEmpty()) {
        corrExprs = getCorrelatedExprs(*mergeClause.getQueryGraphCollection(), predicates,
            plan.getSchema());
    }
    auto existenceMark = mergeClause.getExistenceMark();
    planOptionalMatch(*mergeClause.getQueryGraphCollection(), predicates, corrExprs, existenceMark,
        plan);
    auto merge =
        std::make_shared<LogicalMerge>(existenceMark, std::move(corrExprs), plan.getLastOperator());
    if (mergeClause.hasInsertNodeInfo()) {
        for (auto& info : mergeClause.getInsertNodeInfos()) {
            merge->addInsertNodeInfo(createLogicalInsertInfo(info)->copy());
        }
    }
    if (mergeClause.hasInsertRelInfo()) {
        for (auto& info : mergeClause.getInsertRelInfos()) {
            merge->addInsertRelInfo(createLogicalInsertInfo(info)->copy());
        }
    }
    if (mergeClause.hasOnCreateSetNodeInfo()) {
        for (auto& info : mergeClause.getOnCreateSetNodeInfos()) {
            merge->addOnCreateSetNodeInfo(info.copy());
        }
    }
    if (mergeClause.hasOnCreateSetRelInfo()) {
        for (auto& info : mergeClause.getOnCreateSetRelInfos()) {
            merge->addOnCreateSetRelInfo(info.copy());
        }
    }
    if (mergeClause.hasOnMatchSetNodeInfo()) {
        for (auto& info : mergeClause.getOnMatchSetNodeInfos()) {
            merge->addOnMatchSetNodeInfo(info.copy());
        }
    }
    if (mergeClause.hasOnMatchSetRelInfo()) {
        for (auto& info : mergeClause.getOnMatchSetRelInfos()) {
            merge->addOnMatchSetRelInfo(info.copy());
        }
    }
    appendFlattens(merge->getGroupsPosToFlatten(), plan);
    merge->setChild(0, plan.getLastOperator());
    merge->computeFactorizedSchema();
    plan.setLastOperator(merge);
}

void Planner::planSetClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    appendAccumulate(plan);
    auto& setClause = updatingClause->constCast<BoundSetClause>();
    if (setClause.hasNodeInfo()) {
        appendSetProperty(setClause.getNodeInfos(), plan);
    }
    if (setClause.hasRelInfo()) {
        appendSetProperty(setClause.getRelInfos(), plan);
    }
}

void Planner::planDeleteClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    appendAccumulate(plan);
    auto& deleteClause = updatingClause->constCast<BoundDeleteClause>();
    if (deleteClause.hasRelInfo()) {
        appendDelete(deleteClause.getRelInfos(), plan);
    }
    if (deleteClause.hasNodeInfo()) {
        appendDelete(deleteClause.getNodeInfos(), plan);
    }
}

} // namespace planner
} // namespace kuzu
