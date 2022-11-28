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
        appendSet((BoundSetClause&)updatingClause, plan);
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

void UpdatePlanner::planSetItem(expression_pair setItem, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto lhs = setItem.first;
    auto rhs = setItem.second;
    // Check LHS
    assert(lhs->getChild(0)->dataType.typeID == NODE);
    auto nodeExpression = static_pointer_cast<NodeExpression>(lhs->getChild(0));
    auto lhsGroupPos = schema->getGroupPos(nodeExpression->getInternalIDPropertyName());
    auto isLhsFlat = schema->getGroup(lhsGroupPos)->getIsFlat();
    // Check RHS
    auto rhsDependentGroupsPos = schema->getDependentGroupsPos(rhs);
    if (!rhsDependentGroupsPos.empty()) { // RHS is not constant
        auto rhsPos = QueryPlanner::appendFlattensButOne(rhsDependentGroupsPos, plan);
        auto isRhsFlat = schema->getGroup(rhsPos)->getIsFlat();
        // If both are unflat and from different groups, we flatten LHS.
        if (!isRhsFlat && !isLhsFlat && lhsGroupPos != rhsPos) {
            QueryPlanner::appendFlattenIfNecessary(lhsGroupPos, plan);
        }
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
    auto schema = plan.getSchema();
    vector<expression_pair> setItems;
    vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeyPairs;
    for (auto& createNode : createNodes) {
        auto node = createNode->getNode();
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
        schema->flattenGroup(groupPos); // create output is always flat
        nodeAndPrimaryKeyPairs.emplace_back(node, createNode->getPrimaryKeyExpression());
        for (auto& setItem : createNode->getSetItems()) {
            setItems.push_back(setItem);
        }
    }
    auto createNode =
        make_shared<LogicalCreateNode>(std::move(nodeAndPrimaryKeyPairs), plan.getLastOperator());
    plan.setLastOperator(createNode);
    appendSet(std::move(setItems), plan);
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
    plan.setLastOperator(createRel);
}

void UpdatePlanner::appendSet(vector<expression_pair> setItems, LogicalPlan& plan) {
    for (auto& setItem : setItems) {
        planSetItem(setItem, plan);
    }
    if (!setItems.empty()) {
        plan.setLastOperator(
            make_shared<LogicalSetNodeProperty>(std::move(setItems), plan.getLastOperator()));
    }
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
    vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeyPairs;
    for (auto& deleteNode : deleteNodes) {
        nodeAndPrimaryKeyPairs.emplace_back(
            deleteNode->getNode(), deleteNode->getPrimaryKeyExpression());
    }
    auto deleteNode =
        make_shared<LogicalDeleteNode>(std::move(nodeAndPrimaryKeyPairs), plan.getLastOperator());
    plan.setLastOperator(std::move(deleteNode));
}

void UpdatePlanner::appendDeleteRel(
    const vector<shared_ptr<RelExpression>>& deleteRels, LogicalPlan& plan) {
    // Delete one rel at a time so we flatten for each rel.
    for (auto& rel : deleteRels) {
        auto srcNodeID = rel->getSrcNode()->getInternalIDProperty();
        QueryPlanner::appendFlattenIfNecessary(srcNodeID, plan);
        auto dstNodeID = rel->getDstNode()->getInternalIDProperty();
        QueryPlanner::appendFlattenIfNecessary(dstNodeID, plan);
    }
    auto deleteRel = make_shared<LogicalDeleteRel>(deleteRels, plan.getLastOperator());
    plan.setLastOperator(std::move(deleteRel));
}

} // namespace planner
} // namespace kuzu
