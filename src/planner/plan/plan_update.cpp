#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_merge_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/logical_plan/logical_operator/logical_merge.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::planUpdatingClause(
    binder::BoundUpdatingClause& updatingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planUpdatingClause(updatingClause, *plan);
    }
}

void QueryPlanner::planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        planCreateClause(updatingClause, plan);
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
        throw NotImplementedException("QueryPlanner::planUpdatingClause");
    }
}

void QueryPlanner::planCreateClause(
    binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    auto& createClause = (BoundCreateClause&)updatingClause;
    if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
        expression_vector expressions;
        for (auto& setItem : createClause.getAllSetItems()) {
            expressions.push_back(setItem.second);
        }
        appendExpressionsScan(expressions, plan);
    } else {
        appendAccumulate(common::AccumulateType::REGULAR, plan);
    }
    if (createClause.hasNodeInfo()) {
        appendCreateNode(createClause.getNodeInfos(), plan);
    }
    if (createClause.hasRelInfo()) {
        appendCreateRel(createClause.getRelInfos(), plan);
    }
}

void QueryPlanner::planSetClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    appendAccumulate(common::AccumulateType::REGULAR, plan);
    auto& setClause = (BoundSetClause&)updatingClause;
    if (setClause.hasNodeInfo()) {
        appendSetNodeProperty(setClause.getNodeInfos(), plan);
    }
    if (setClause.hasRelInfo()) {
        appendSetRelProperty(setClause.getRelInfos(), plan);
    }
}

void QueryPlanner::planDeleteClause(
    binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    appendAccumulate(common::AccumulateType::REGULAR, plan);
    auto& deleteClause = (BoundDeleteClause&)updatingClause;
    if (deleteClause.hasRelInfo()) {
        appendDeleteRel(deleteClause.getRelInfos(), plan);
    }
    if (deleteClause.hasNodeInfo()) {
        appendDeleteNode(deleteClause.getNodeInfos(), plan);
    }
}

static std::vector<std::unique_ptr<LogicalCreateNodeInfo>> populateCreateNodeInfos(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos) {
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> result;
    for (auto& info : boundCreateInfos) {
        auto node = std::static_pointer_cast<NodeExpression>(info->nodeOrRel);
        auto extraInfo = (ExtraCreateNodeInfo*)info->extraInfo.get();
        result.push_back(std::make_unique<LogicalCreateNodeInfo>(node, extraInfo->primaryKey));
    }
    return result;
}

static std::vector<std::unique_ptr<LogicalCreateRelInfo>> populateCreateRelInfos(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos) {
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> result;
    for (auto& info : boundCreateInfos) {
        auto rel = std::static_pointer_cast<RelExpression>(info->nodeOrRel);
        result.push_back(std::make_unique<LogicalCreateRelInfo>(rel, info->setItems));
    }
    return result;
}

void QueryPlanner::appendCreateNode(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos, LogicalPlan& plan) {
    auto logicalCreateInfos = populateCreateNodeInfos(boundCreateInfos);
    auto createNode =
        std::make_shared<LogicalCreateNode>(std::move(logicalCreateInfos), plan.getLastOperator());
    appendFlattens(createNode->getGroupsPosToFlatten(), plan);
    createNode->setChild(0, plan.getLastOperator());
    createNode->computeFactorizedSchema();
    plan.setLastOperator(createNode);
    // Apply SET after CREATE
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> boundSetInfos;
    for (auto& boundCreateInfo : boundCreateInfos) {
        auto node = std::static_pointer_cast<NodeExpression>(boundCreateInfo->nodeOrRel);
        for (auto& setItem : boundCreateInfo->setItems) {
            boundSetInfos.push_back(
                std::make_unique<BoundSetPropertyInfo>(UpdateTableType::NODE, node, setItem));
        }
    }
    std::vector<BoundSetPropertyInfo*> setInfoPtrs;
    for (auto& setInfo : boundSetInfos) {
        setInfoPtrs.push_back(setInfo.get());
    }
    appendSetNodeProperty(setInfoPtrs, plan);
}

void QueryPlanner::appendCreateRel(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos, LogicalPlan& plan) {
    auto logicalCreateInfos = populateCreateRelInfos(boundCreateInfos);
    auto createRel =
        std::make_shared<LogicalCreateRel>(std::move(logicalCreateInfos), plan.getLastOperator());
    appendFlattens(createRel->getGroupsPosToFlatten(), plan);
    createRel->setChild(0, plan.getLastOperator());
    createRel->computeFactorizedSchema();
    plan.setLastOperator(createRel);
}

void QueryPlanner::planMergeClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    auto& mergeClause = (BoundMergeClause&)updatingClause;
    auto queryGraphCollection = mergeClause.getQueryGraphCollection();
    auto predicates = mergeClause.getPredicate()->splitOnAND();
    planOptionalMatch(*mergeClause.getQueryGraphCollection(), predicates, plan);
    // TODO: (Xiyang) fix mark
    auto mark = queryGraphCollection->getQueryNodes()[0]->getInternalIDProperty();
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> logicalCreateNodeInfos;
    if (mergeClause.hasNodeCreateInfo()) {
        logicalCreateNodeInfos = populateCreateNodeInfos(mergeClause.getNodeCreateInfos());
    }
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> logicalCreateRelInfos;
    if (mergeClause.hasRelCreateInfo()) {
        logicalCreateRelInfos = populateCreateRelInfos(mergeClause.getRelCreateInfos());
    }
    auto merge = std::make_shared<LogicalMerge>(mark, std::move(logicalCreateNodeInfos),
        std::move(logicalCreateRelInfos), plan.getLastOperator());
    appendFlattens(merge->getGroupsPosToFlatten(), plan);
    merge->setChild(0, plan.getLastOperator());
    merge->computeFactorizedSchema();
    plan.setLastOperator(merge);
}

} // namespace planner
} // namespace kuzu
