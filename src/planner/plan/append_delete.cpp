#include "binder/query/updating_clause/bound_delete_clause.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendDeleteNode(
    const std::vector<binder::BoundDeleteInfo*>& infos, LogicalPlan& plan) {
    std::vector<std::shared_ptr<NodeExpression>> nodes;
    expression_vector primaryKeys;
    for (auto& info : infos) {
        auto node = std::static_pointer_cast<NodeExpression>(info->nodeOrRel);
        auto extraInfo = (ExtraDeleteNodeInfo*)info->extraInfo.get();
        nodes.push_back(node);
        primaryKeys.push_back(extraInfo->primaryKey);
    }
    auto deleteNode = std::make_shared<LogicalDeleteNode>(
        std::move(nodes), std::move(primaryKeys), plan.getLastOperator());
    deleteNode->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteNode));
}

void QueryPlanner::appendDeleteRel(
    const std::vector<binder::BoundDeleteInfo*>& infos, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> deleteRels;
    for (auto& info : infos) {
        auto rel = std::static_pointer_cast<RelExpression>(info->nodeOrRel);
        deleteRels.push_back(rel);
    }
    auto deleteRel = std::make_shared<LogicalDeleteRel>(deleteRels, plan.getLastOperator());
    for (auto i = 0u; i < deleteRel->getNumRels(); ++i) {
        appendFlattens(deleteRel->getGroupsPosToFlatten(i), plan);
        deleteRel->setChild(0, plan.getLastOperator());
    }
    deleteRel->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteRel));
}

} // namespace planner
} // namespace kuzu
