#include "binder/query/updating_clause/bound_delete_clause.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendDeleteNode(
    const std::vector<std::unique_ptr<binder::BoundDeleteNode>>& deleteNodes, LogicalPlan& plan) {
    std::vector<std::shared_ptr<NodeExpression>> nodes;
    expression_vector primaryKeys;
    for (auto& deleteNode : deleteNodes) {
        nodes.push_back(deleteNode->getNode());
        primaryKeys.push_back(deleteNode->getPrimaryKeyExpression());
    }
    auto deleteNode = std::make_shared<LogicalDeleteNode>(
        std::move(nodes), std::move(primaryKeys), plan.getLastOperator());
    deleteNode->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteNode));
}

void QueryPlanner::appendDeleteRel(
    const std::vector<std::shared_ptr<binder::RelExpression>>& deleteRels, LogicalPlan& plan) {
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
