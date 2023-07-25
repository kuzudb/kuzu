#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendCreateNode(
    const std::vector<std::unique_ptr<binder::BoundCreateNode>>& createNodes, LogicalPlan& plan) {
    std::vector<std::shared_ptr<NodeExpression>> nodes;
    expression_vector primaryKeys;
    std::vector<std::unique_ptr<BoundSetNodeProperty>> setNodeProperties;
    for (auto& createNode : createNodes) {
        auto node = createNode->getNode();
        nodes.push_back(node);
        primaryKeys.push_back(createNode->getPrimaryKeyExpression());
        for (auto& setItem : createNode->getSetItems()) {
            setNodeProperties.push_back(std::make_unique<BoundSetNodeProperty>(node, setItem));
        }
    }
    auto createNode = std::make_shared<LogicalCreateNode>(
        std::move(nodes), std::move(primaryKeys), plan.getLastOperator());
    appendFlattens(createNode->getGroupsPosToFlatten(), plan);
    createNode->setChild(0, plan.getLastOperator());
    createNode->computeFactorizedSchema();
    plan.setLastOperator(createNode);
    appendSetNodeProperty(setNodeProperties, plan);
}

void QueryPlanner::appendCreateRel(
    const std::vector<std::unique_ptr<binder::BoundCreateRel>>& createRels, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> rels;
    std::vector<std::vector<expression_pair>> setItemsPerRel;
    for (auto& createRel : createRels) {
        rels.push_back(createRel->getRel());
        setItemsPerRel.push_back(createRel->getSetItems());
    }
    auto createRel = std::make_shared<LogicalCreateRel>(
        std::move(rels), std::move(setItemsPerRel), plan.getLastOperator());
    appendFlattens(createRel->getGroupsPosToFlatten(), plan);
    createRel->setChild(0, plan.getLastOperator());
    createRel->computeFactorizedSchema();
    plan.setLastOperator(createRel);
}

} // namespace planner
} // namespace kuzu
