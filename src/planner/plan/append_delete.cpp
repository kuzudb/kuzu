#include "binder/query/updating_clause/bound_delete_info.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendDeleteNode(
    const std::vector<BoundDeleteInfo*>& boundInfos, LogicalPlan& plan) {
    std::vector<std::shared_ptr<binder::NodeExpression>> nodes;
    for (auto& boundInfo : boundInfos) {
        nodes.push_back(std::static_pointer_cast<NodeExpression>(boundInfo->nodeOrRel));
    }
    auto deleteNode = std::make_shared<LogicalDeleteNode>(std::move(nodes), plan.getLastOperator());
    appendFlattens(deleteNode->getGroupsPosToFlatten(), plan);
    deleteNode->setChild(0, plan.getLastOperator());
    deleteNode->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteNode));
}

void QueryPlanner::appendDeleteRel(
    const std::vector<BoundDeleteInfo*>& boundInfos, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> rels;
    for (auto& info : boundInfos) {
        auto rel = std::static_pointer_cast<RelExpression>(info->nodeOrRel);
        rels.push_back(rel);
    }
    auto deleteRel = std::make_shared<LogicalDeleteRel>(rels, plan.getLastOperator());
    for (auto i = 0u; i < boundInfos.size(); ++i) {
        appendFlattens(deleteRel->getGroupsPosToFlatten(i), plan);
        deleteRel->setChild(0, plan.getLastOperator());
    }
    deleteRel->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteRel));
}

} // namespace planner
} // namespace kuzu
