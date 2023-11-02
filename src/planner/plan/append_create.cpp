#include "binder/query/updating_clause/bound_insert_info.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalInsertNodeInfo> QueryPlanner::createLogicalInsertNodeInfo(
    BoundInsertInfo* boundInsertInfo) {
    auto node = std::static_pointer_cast<NodeExpression>(boundInsertInfo->nodeOrRel);
    auto propertiesToReturn = getProperties(*node);
    return std::make_unique<LogicalInsertNodeInfo>(
        node, boundInsertInfo->setItems, std::move(propertiesToReturn));
}

std::unique_ptr<LogicalInsertRelInfo> QueryPlanner::createLogicalInsertRelInfo(
    BoundInsertInfo* boundInsertInfo) {
    auto rel = std::static_pointer_cast<RelExpression>(boundInsertInfo->nodeOrRel);
    auto propertiesToReturn = getProperties(*rel);
    return std::make_unique<LogicalInsertRelInfo>(
        rel, boundInsertInfo->setItems, std::move(propertiesToReturn));
}

void QueryPlanner::appendInsertNode(
    const std::vector<binder::BoundInsertInfo*>& boundInsertInfos, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalInsertNodeInfo>> logicalInfos;
    logicalInfos.reserve(boundInsertInfos.size());
    for (auto& boundInfo : boundInsertInfos) {
        logicalInfos.push_back(createLogicalInsertNodeInfo(boundInfo));
    }
    auto insertNode =
        std::make_shared<LogicalInsertNode>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(insertNode->getGroupsPosToFlatten(), plan);
    insertNode->setChild(0, plan.getLastOperator());
    insertNode->computeFactorizedSchema();
    plan.setLastOperator(insertNode);
}

void QueryPlanner::appendInsertRel(
    const std::vector<binder::BoundInsertInfo*>& boundInsertInfos, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalInsertRelInfo>> logicalInfos;
    logicalInfos.reserve(boundInsertInfos.size());
    for (auto& boundInfo : boundInsertInfos) {
        logicalInfos.push_back(createLogicalInsertRelInfo(boundInfo));
    }
    auto insertRel =
        std::make_shared<LogicalInsertRel>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(insertRel->getGroupsPosToFlatten(), plan);
    insertRel->setChild(0, plan.getLastOperator());
    insertRel->computeFactorizedSchema();
    plan.setLastOperator(insertRel);
}
} // namespace planner
} // namespace kuzu
