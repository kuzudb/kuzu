#include "planner/update_planner.h"

#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "planner/logical_plan/logical_operator/sink_util.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void UpdatePlanner::planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        auto& createClause = (BoundCreateClause&)updatingClause;
        if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
            expression_vector expressions;
            for (auto& setItem : createClause.getAllSetItems()) {
                expressions.push_back(setItem.second);
            }
            queryPlanner->appendExpressionsScan(expressions, plan);
        } else {
            queryPlanner->appendAccumulate(plan);
        }
        planCreate((BoundCreateClause&)updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        queryPlanner->appendAccumulate(plan);
        planSet((BoundSetClause&)updatingClause, plan);
        return;
    }
    case ClauseType::DELETE_: {
        queryPlanner->appendAccumulate(plan);
        planDelete((BoundDeleteClause&)updatingClause, plan);
        return;
    }
    default:
        assert(false);
    }
}

void UpdatePlanner::planCreate(BoundCreateClause& createClause, LogicalPlan& plan) {
    if (createClause.hasCreateNode()) {
        appendCreateNode(createClause.getCreateNodes(), plan);
    }
    if (createClause.hasCreateRel()) {
        appendCreateRel(createClause.getCreateRels(), plan);
    }
}

void UpdatePlanner::appendCreateNode(
    const std::vector<std::unique_ptr<BoundCreateNode>>& createNodes, LogicalPlan& plan) {
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
    queryPlanner->appendFlattens(createNode->getGroupsPosToFlatten(), plan);
    createNode->setChild(0, plan.getLastOperator());
    createNode->computeFactorizedSchema();
    plan.setLastOperator(createNode);
    appendSetNodeProperty(setNodeProperties, plan);
}

void UpdatePlanner::appendCreateRel(
    const std::vector<std::unique_ptr<BoundCreateRel>>& createRels, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> rels;
    std::vector<std::vector<expression_pair>> setItemsPerRel;
    for (auto& createRel : createRels) {
        rels.push_back(createRel->getRel());
        setItemsPerRel.push_back(createRel->getSetItems());
    }
    auto createRel = std::make_shared<LogicalCreateRel>(
        std::move(rels), std::move(setItemsPerRel), plan.getLastOperator());
    queryPlanner->appendFlattens(createRel->getGroupsPosToFlatten(), plan);
    createRel->setChild(0, plan.getLastOperator());
    createRel->computeFactorizedSchema();
    plan.setLastOperator(createRel);
}

void UpdatePlanner::planSet(BoundSetClause& setClause, LogicalPlan& plan) {
    if (setClause.hasSetNodeProperty()) {
        appendSetNodeProperty(setClause.getSetNodeProperties(), plan);
    }
    if (setClause.hasSetRelProperty()) {
        appendSetRelProperty(setClause.getSetRelProperties(), plan);
    }
}

void UpdatePlanner::appendSetNodeProperty(
    const std::vector<std::unique_ptr<BoundSetNodeProperty>>& setNodeProperties,
    LogicalPlan& plan) {
    std::vector<std::shared_ptr<NodeExpression>> nodes;
    std::vector<expression_pair> setItems;
    for (auto& setNodeProperty : setNodeProperties) {
        nodes.push_back(setNodeProperty->getNode());
        setItems.push_back(setNodeProperty->getSetItem());
    }
    for (auto i = 0u; i < setItems.size(); ++i) {
        auto lhsNodeID = nodes[i]->getInternalIDProperty();
        auto rhs = setItems[i].second;
        // flatten rhs
        auto rhsDependentGroupsPos = plan.getSchema()->getDependentGroupsPos(rhs);
        auto rhsGroupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
            rhsDependentGroupsPos, plan.getSchema());
        queryPlanner->appendFlattens(rhsGroupsPosToFlatten, plan);
        // flatten lhs if needed
        auto lhsGroupPos = plan.getSchema()->getGroupPos(*lhsNodeID);
        auto rhsLeadingGroupPos =
            SchemaUtils::getLeadingGroupPos(rhsDependentGroupsPos, *plan.getSchema());
        if (lhsGroupPos != rhsLeadingGroupPos) {
            queryPlanner->appendFlattenIfNecessary(lhsGroupPos, plan);
        }
    }
    auto setNodeProperty = std::make_shared<LogicalSetNodeProperty>(
        std::move(nodes), std::move(setItems), plan.getLastOperator());
    setNodeProperty->computeFactorizedSchema();
    plan.setLastOperator(setNodeProperty);
}

void UpdatePlanner::appendSetRelProperty(
    const std::vector<std::unique_ptr<BoundSetRelProperty>>& setRelProperties, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> rels;
    std::vector<expression_pair> setItems;
    for (auto& setRelProperty : setRelProperties) {
        rels.push_back(setRelProperty->getRel());
        setItems.push_back(setRelProperty->getSetItem());
    }
    auto setRelProperty = std::make_shared<LogicalSetRelProperty>(
        std::move(rels), std::move(setItems), plan.getLastOperator());
    for (auto i = 0u; i < setRelProperty->getNumRels(); ++i) {
        queryPlanner->appendFlattens(setRelProperty->getGroupsPosToFlatten(i), plan);
        setRelProperty->setChild(0, plan.getLastOperator());
    }
    setRelProperty->computeFactorizedSchema();
    plan.setLastOperator(setRelProperty);
}

void UpdatePlanner::planDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan) {
    if (deleteClause.hasDeleteRel()) {
        appendDeleteRel(deleteClause.getDeleteRels(), plan);
    }
    if (deleteClause.hasDeleteNode()) {
        appendDeleteNode(deleteClause.getDeleteNodes(), plan);
    }
}

void UpdatePlanner::appendDeleteNode(
    const std::vector<std::unique_ptr<BoundDeleteNode>>& deleteNodes, LogicalPlan& plan) {
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

void UpdatePlanner::appendDeleteRel(
    const std::vector<std::shared_ptr<RelExpression>>& deleteRels, LogicalPlan& plan) {
    auto deleteRel = std::make_shared<LogicalDeleteRel>(deleteRels, plan.getLastOperator());
    for (auto i = 0u; i < deleteRel->getNumRels(); ++i) {
        queryPlanner->appendFlattens(deleteRel->getGroupsPosToFlatten(i), plan);
        deleteRel->setChild(0, plan.getLastOperator());
    }
    deleteRel->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteRel));
}

} // namespace planner
} // namespace kuzu
