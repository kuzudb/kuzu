#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_insert_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "common/enums/join_type.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::planUpdatingClause(
    const BoundUpdatingClause* updatingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
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
    auto insertClause =
        ku_dynamic_cast<const BoundUpdatingClause*, const BoundInsertClause*>(updatingClause);
    if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
        appendDummyScan(plan);
    } else {
        appendAccumulate(AccumulateType::REGULAR, plan);
    }
    if (insertClause->hasNodeInfo()) {
        appendInsertNode(insertClause->getNodeInfos(), plan);
    }
    if (insertClause->hasRelInfo()) {
        appendInsertRel(insertClause->getRelInfos(), plan);
    }
}

void Planner::planMergeClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    auto mergeClause =
        ku_dynamic_cast<const BoundUpdatingClause*, const BoundMergeClause*>(updatingClause);
    binder::expression_vector predicates;
    if (mergeClause->hasPredicate()) {
        predicates = mergeClause->getPredicate()->splitOnAND();
    }
    planOptionalMatch(*mergeClause->getQueryGraphCollection(), predicates, plan);
    std::shared_ptr<Expression> mark;
    auto& createInfos = mergeClause->getInsertInfosRef();
    KU_ASSERT(!createInfos.empty());
    auto& createInfo = createInfos[0];
    switch (createInfo.tableType) {
    case TableType::NODE: {
        auto node = (NodeExpression*)createInfo.pattern.get();
        mark = node->getInternalID();
    } break;
    case TableType::REL: {
        auto rel = (RelExpression*)createInfo.pattern.get();
        mark = rel->getInternalIDProperty();
    } break;
    default:
        KU_UNREACHABLE;
    }
    std::vector<LogicalInsertInfo> logicalInsertNodeInfos;
    if (mergeClause->hasInsertNodeInfo()) {
        auto boundInsertNodeInfos = mergeClause->getInsertNodeInfos();
        for (auto& info : boundInsertNodeInfos) {
            logicalInsertNodeInfos.push_back(createLogicalInsertInfo(info)->copy());
        }
    }
    std::vector<LogicalInsertInfo> logicalInsertRelInfos;
    if (mergeClause->hasInsertRelInfo()) {
        for (auto& info : mergeClause->getInsertRelInfos()) {
            logicalInsertRelInfos.push_back(createLogicalInsertInfo(info)->copy());
        }
    }
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> logicalOnCreateSetNodeInfos;
    if (mergeClause->hasOnCreateSetNodeInfo()) {
        for (auto& info : mergeClause->getOnCreateSetNodeInfos()) {
            logicalOnCreateSetNodeInfos.push_back(createLogicalSetPropertyInfo(info));
        }
    }
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> logicalOnCreateSetRelInfos;
    if (mergeClause->hasOnCreateSetRelInfo()) {
        for (auto& info : mergeClause->getOnCreateSetRelInfos()) {
            logicalOnCreateSetRelInfos.push_back(createLogicalSetPropertyInfo(info));
        }
    }
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> logicalOnMatchSetNodeInfos;
    if (mergeClause->hasOnMatchSetNodeInfo()) {
        for (auto& info : mergeClause->getOnMatchSetNodeInfos()) {
            logicalOnMatchSetNodeInfos.push_back(createLogicalSetPropertyInfo(info));
        }
    }
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> logicalOnMatchSetRelInfos;
    if (mergeClause->hasOnMatchSetRelInfo()) {
        for (auto& info : mergeClause->getOnMatchSetRelInfos()) {
            logicalOnMatchSetRelInfos.push_back(createLogicalSetPropertyInfo(info));
        }
    }
    auto merge = std::make_shared<LogicalMerge>(mark, std::move(logicalInsertNodeInfos),
        std::move(logicalInsertRelInfos), std::move(logicalOnCreateSetNodeInfos),
        std::move(logicalOnCreateSetRelInfos), std::move(logicalOnMatchSetNodeInfos),
        std::move(logicalOnMatchSetRelInfos), plan.getLastOperator());
    appendFlattens(merge->getGroupsPosToFlatten(), plan);
    merge->setChild(0, plan.getLastOperator());
    merge->computeFactorizedSchema();
    plan.setLastOperator(merge);
}

void Planner::planSetClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    appendAccumulate(AccumulateType::REGULAR, plan);
    auto setClause =
        ku_dynamic_cast<const BoundUpdatingClause*, const BoundSetClause*>(updatingClause);
    if (setClause->hasNodeInfo()) {
        appendSetNodeProperty(setClause->getNodeInfos(), plan);
    }
    if (setClause->hasRelInfo()) {
        appendSetRelProperty(setClause->getRelInfos(), plan);
    }
}

void Planner::planDeleteClause(const BoundUpdatingClause* updatingClause, LogicalPlan& plan) {
    appendAccumulate(AccumulateType::REGULAR, plan);
    auto deleteClause =
        ku_dynamic_cast<const BoundUpdatingClause*, const BoundDeleteClause*>(updatingClause);
    if (deleteClause->hasRelInfo()) {
        appendDeleteRel(deleteClause->getRelInfos(), plan);
    }
    if (deleteClause->hasNodeInfo()) {
        appendDeleteNode(deleteClause->getNodeInfos(), plan);
    }
}

} // namespace planner
} // namespace kuzu
