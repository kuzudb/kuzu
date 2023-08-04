#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

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

// TODO(Xiyang): remove this after refactoring
static std::vector<std::unique_ptr<BoundSetPropertyInfo>> populateBoundSetNodePropertyInfos(
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
    auto boundSetNodePropertyInfos = populateBoundSetNodePropertyInfos(boundCreateInfos);
    std::vector<BoundSetPropertyInfo*> setInfoPtrs;
    for (auto& setInfo : boundSetNodePropertyInfos) {
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
} // namespace planner
} // namespace kuzu
