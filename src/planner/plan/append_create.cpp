#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/logical_plan/persistent/logical_create.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalCreateNodeInfo> QueryPlanner::createLogicalCreateNodeInfo(
    BoundCreateInfo* boundCreateInfo) {
    auto node = std::static_pointer_cast<NodeExpression>(boundCreateInfo->nodeOrRel);
    auto propertiesToReturn = getProperties(*node);
    return std::make_unique<LogicalCreateNodeInfo>(
        node, boundCreateInfo->setItems, std::move(propertiesToReturn));
}

std::unique_ptr<LogicalCreateRelInfo> QueryPlanner::createLogicalCreateRelInfo(
    BoundCreateInfo* boundCreateInfo) {
    auto rel = std::static_pointer_cast<RelExpression>(boundCreateInfo->nodeOrRel);
    auto propertiesToReturn = getProperties(*rel);
    return std::make_unique<LogicalCreateRelInfo>(
        rel, boundCreateInfo->setItems, std::move(propertiesToReturn));
}

std::vector<std::unique_ptr<BoundSetPropertyInfo>> QueryPlanner::createLogicalSetPropertyInfo(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos) {
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> result;
    for (auto& boundCreateInfo : boundCreateInfos) {
        auto node = std::static_pointer_cast<NodeExpression>(boundCreateInfo->nodeOrRel);
        for (auto& setItem : boundCreateInfo->setItems) {
            result.push_back(
                std::make_unique<BoundSetPropertyInfo>(UpdateTableType::NODE, node, setItem));
        }
    }
    return result;
}

void QueryPlanner::appendCreateNode(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> logicalInfos;
    logicalInfos.reserve(boundCreateInfos.size());
    for (auto& boundInfo : boundCreateInfos) {
        logicalInfos.push_back(createLogicalCreateNodeInfo(boundInfo));
    }
    auto createNode =
        std::make_shared<LogicalCreateNode>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(createNode->getGroupsPosToFlatten(), plan);
    createNode->setChild(0, plan.getLastOperator());
    createNode->computeFactorizedSchema();
    plan.setLastOperator(createNode);
}

void QueryPlanner::appendCreateRel(
    const std::vector<binder::BoundCreateInfo*>& boundCreateInfos, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> logicalInfos;
    logicalInfos.reserve(boundCreateInfos.size());
    for (auto& boundInfo : boundCreateInfos) {
        logicalInfos.push_back(createLogicalCreateRelInfo(boundInfo));
    }
    auto createRel =
        std::make_shared<LogicalCreateRel>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(createRel->getGroupsPosToFlatten(), plan);
    createRel->setChild(0, plan.getLastOperator());
    createRel->computeFactorizedSchema();
    plan.setLastOperator(createRel);
}
} // namespace planner
} // namespace kuzu
