#include "planner/update_planner.h"

#include "planner/logical_plan/logical_operator/logical_create.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "planner/logical_plan/logical_operator/sink_util.h"
#include "planner/query_planner.h"

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
            QueryPlanner::appendExpressionsScan(expressions, plan);
        } else {
            QueryPlanner::appendAccumulate(plan);
        }
        planCreate((BoundCreateClause&)updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        QueryPlanner::appendAccumulate(plan);
        planSet((BoundSetClause&)updatingClause, plan);
        return;
    }
    case ClauseType::DELETE: {
        QueryPlanner::appendAccumulate(plan);
        planDelete((BoundDeleteClause&)updatingClause, plan);
        return;
    }
    default:
        assert(false);
    }
}

void UpdatePlanner::planCreate(BoundCreateClause& createClause, LogicalPlan& plan) {
    // Flatten all inputs. E.g. MATCH (a) CREATE (b). We need to create b for each tuple in the
    // match clause. This is to simplify operator implementation.
    for (auto groupPos = 0u; groupPos < plan.getSchema()->getNumGroups(); ++groupPos) {
        QueryPlanner::appendFlattenIfNecessary(groupPos, plan);
    }
    if (createClause.hasCreateNode()) {
        appendCreateNode(createClause.getCreateNodes(), plan);
    }
    if (createClause.hasCreateRel()) {
        appendCreateRel(createClause.getCreateRels(), plan);
    }
}

void UpdatePlanner::appendCreateNode(
    const vector<unique_ptr<BoundCreateNode>>& createNodes, LogicalPlan& plan) {
    vector<shared_ptr<NodeExpression>> nodes;
    expression_vector primaryKeys;
    vector<unique_ptr<BoundSetNodeProperty>> setNodeProperties;
    for (auto& createNode : createNodes) {
        auto node = createNode->getNode();
        nodes.push_back(node);
        primaryKeys.push_back(createNode->getPrimaryKeyExpression());
        for (auto& setItem : createNode->getSetItems()) {
            setNodeProperties.push_back(make_unique<BoundSetNodeProperty>(node, setItem));
        }
    }
    auto createNode = make_shared<LogicalCreateNode>(
        std::move(nodes), std::move(primaryKeys), plan.getLastOperator());
    createNode->computeSchema();
    plan.setLastOperator(createNode);
    appendSetNodeProperty(setNodeProperties, plan);
}

void UpdatePlanner::appendCreateRel(
    const vector<unique_ptr<BoundCreateRel>>& createRels, LogicalPlan& plan) {
    vector<shared_ptr<RelExpression>> rels;
    vector<vector<expression_pair>> setItemsPerRel;
    for (auto& createRel : createRels) {
        rels.push_back(createRel->getRel());
        setItemsPerRel.push_back(createRel->getSetItems());
    }
    auto createRel = make_shared<LogicalCreateRel>(
        std::move(rels), std::move(setItemsPerRel), plan.getLastOperator());
    createRel->computeSchema();
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
    const vector<unique_ptr<BoundSetNodeProperty>>& setNodeProperties, LogicalPlan& plan) {
    vector<shared_ptr<NodeExpression>> nodes;
    vector<expression_pair> setItems;
    for (auto& setNodeProperty : setNodeProperties) {
        auto node = setNodeProperty->getNode();
        auto lhsGroupPos = plan.getSchema()->getGroupPos(node->getInternalIDPropertyName());
        auto isLhsFlat = plan.getSchema()->getGroup(lhsGroupPos)->isFlat();
        auto rhs = setNodeProperty->getSetItem().second;
        auto rhsDependentGroupsPos = plan.getSchema()->getDependentGroupsPos(rhs);
        if (!rhsDependentGroupsPos.empty()) { // RHS is not constant
            auto rhsPos = QueryPlanner::appendFlattensButOne(rhsDependentGroupsPos, plan);
            auto isRhsFlat = plan.getSchema()->getGroup(rhsPos)->isFlat();
            // If both are unflat and from different groups, we flatten LHS.
            if (!isRhsFlat && !isLhsFlat && lhsGroupPos != rhsPos) {
                QueryPlanner::appendFlattenIfNecessary(lhsGroupPos, plan);
            }
        }
        nodes.push_back(node);
        setItems.push_back(setNodeProperty->getSetItem());
    }
    auto setNodeProperty = make_shared<LogicalSetNodeProperty>(
        std::move(nodes), std::move(setItems), plan.getLastOperator());
    setNodeProperty->computeSchema();
    plan.setLastOperator(setNodeProperty);
}

void UpdatePlanner::appendSetRelProperty(
    const vector<unique_ptr<BoundSetRelProperty>>& setRelProperties, LogicalPlan& plan) {
    vector<shared_ptr<RelExpression>> rels;
    vector<expression_pair> setItems;
    for (auto& setRelProperty : setRelProperties) {
        flattenRel(*setRelProperty->getRel(), plan);
        auto rhs = setRelProperty->getSetItem().second;
        auto rhsDependentGroupsPos = plan.getSchema()->getDependentGroupsPos(rhs);
        QueryPlanner::appendFlattens(rhsDependentGroupsPos, plan);
        rels.push_back(setRelProperty->getRel());
        setItems.push_back(setRelProperty->getSetItem());
    }
    auto setRelProperty = make_shared<LogicalSetRelProperty>(
        std::move(rels), std::move(setItems), plan.getLastOperator());
    setRelProperty->computeSchema();
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
    const vector<unique_ptr<BoundDeleteNode>>& deleteNodes, LogicalPlan& plan) {
    vector<shared_ptr<NodeExpression>> nodes;
    expression_vector primaryKeys;
    for (auto& deleteNode : deleteNodes) {
        nodes.push_back(deleteNode->getNode());
        primaryKeys.push_back(deleteNode->getPrimaryKeyExpression());
    }
    auto deleteNode = make_shared<LogicalDeleteNode>(
        std::move(nodes), std::move(primaryKeys), plan.getLastOperator());
    deleteNode->computeSchema();
    plan.setLastOperator(std::move(deleteNode));
}

void UpdatePlanner::appendDeleteRel(
    const vector<shared_ptr<RelExpression>>& deleteRels, LogicalPlan& plan) {
    // Delete one rel at a time so we flatten for each rel.
    for (auto& rel : deleteRels) {
        flattenRel(*rel, plan);
    }
    auto deleteRel = make_shared<LogicalDeleteRel>(deleteRels, plan.getLastOperator());
    deleteRel->computeSchema();
    plan.setLastOperator(std::move(deleteRel));
}

void UpdatePlanner::flattenRel(const RelExpression& rel, LogicalPlan& plan) {
    auto srcNodeID = rel.getSrcNode()->getInternalIDProperty();
    QueryPlanner::appendFlattenIfNecessary(srcNodeID, plan);
    auto dstNodeID = rel.getDstNode()->getInternalIDProperty();
    QueryPlanner::appendFlattenIfNecessary(dstNodeID, plan);
}

} // namespace planner
} // namespace kuzu
