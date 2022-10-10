#include "include/update_planner.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/logical_operator/include/logical_create.h"
#include "src/planner/logical_plan/logical_operator/include/logical_delete.h"
#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

namespace graphflow {
namespace planner {

void UpdatePlanner::planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        auto& createClause = (BoundCreateClause&)updatingClause;
        if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
            expression_vector expressions;
            for (auto i = 0u; i < createClause.getNumNodeUpdateInfo(); ++i) {
                auto nodeUpdateInfo = createClause.getNodeUpdateInfo(i);
                for (auto j = 0u; j < nodeUpdateInfo->getNumPropertyUpdateInfo(); ++j) {
                    expressions.push_back(nodeUpdateInfo->getPropertyUpdateInfo(j)->getTarget());
                }
            }
            Enumerator::appendExpressionsScan(expressions, plan);
        } else {
            Enumerator::appendAccumulate(plan);
        }
        appendCreate((BoundCreateClause&)updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        Enumerator::appendAccumulate(plan);
        appendSet((BoundSetClause&)updatingClause, plan);
        return;
    }
    case ClauseType::DELETE: {
        Enumerator::appendAccumulate(plan);
        appendDelete((BoundDeleteClause&)updatingClause, plan);
        return;
    }
    default:
        assert(false);
    }
}

void UpdatePlanner::planPropertyUpdateInfo(
    shared_ptr<Expression> property, shared_ptr<Expression> target, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    // Check LHS
    assert(property->getChild(0)->dataType.typeID == NODE);
    auto nodeExpression = static_pointer_cast<NodeExpression>(property->getChild(0));
    auto originGroupPos = schema->getGroupPos(nodeExpression->getIDProperty());
    auto isOriginFlat = schema->getGroup(originGroupPos)->getIsFlat();
    // Check RHS
    auto targetDependentGroupsPos = schema->getDependentGroupsPos(target);
    if (!targetDependentGroupsPos.empty()) { // RHS is not constant
        auto targetPos = Enumerator::appendFlattensButOne(targetDependentGroupsPos, plan);
        auto isTargetFlat = schema->getGroup(targetPos)->getIsFlat();
        // If both are unflat and from different groups, we flatten LHS.
        if (!isTargetFlat && !isOriginFlat && targetPos != originGroupPos) {
            Enumerator::appendFlattenIfNecessary(originGroupPos, plan);
        }
    }
}

void UpdatePlanner::appendCreate(BoundCreateClause& createClause, LogicalPlan& plan) {
    // Flatten all inputs.
    for (auto groupPos = 0u; groupPos < plan.getSchema()->getNumGroups(); ++groupPos) {
        Enumerator::appendFlattenIfNecessary(groupPos, plan);
    }
    vector<create_item> createItems;
    for (auto i = 0u; i < createClause.getNumNodeUpdateInfo(); ++i) {
        auto nodeUpdateInfo = createClause.getNodeUpdateInfo(i);
        assert(nodeUpdateInfo->getNumPropertyUpdateInfo() != 0);
        vector<expression_pair> setItems;
        for (auto j = 0u; j < nodeUpdateInfo->getNumPropertyUpdateInfo(); ++j) {
            auto propertyUpdateInfo = nodeUpdateInfo->getPropertyUpdateInfo(j);
            auto property = propertyUpdateInfo->getProperty();
            auto target = propertyUpdateInfo->getTarget();
            setItems.emplace_back(property, target);
        }
        createItems.emplace_back(nodeUpdateInfo->getNodeExpression(), move(setItems));
    }
    auto create = make_shared<LogicalCreate>(move(createItems), plan.getLastOperator());
    plan.appendOperator(create);
}

void UpdatePlanner::appendSet(BoundSetClause& setClause, LogicalPlan& plan) {
    vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> setItems;
    for (auto i = 0u; i < setClause.getNumPropertyUpdateInfos(); ++i) {
        auto propertyUpdateInfo = setClause.getPropertyUpdateInfo(i);
        auto property = propertyUpdateInfo->getProperty();
        auto target = propertyUpdateInfo->getTarget();
        planPropertyUpdateInfo(property, target, plan);
        setItems.emplace_back(property, target);
    }
    auto set = make_shared<LogicalSet>(move(setItems), plan.getLastOperator());
    plan.appendOperator(set);
}

void UpdatePlanner::appendDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan) {
    expression_vector nodeExpressions;
    expression_vector primaryKeyExpressions;
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto expression = deleteClause.getExpression(i);
        assert(expression->dataType.typeID == NODE);
        auto& nodeExpression = (NodeExpression&)*expression;
        auto pk =
            catalog.getReadOnlyVersion()->getNodePrimaryKeyProperty(nodeExpression.getTableID());
        auto pkExpression =
            make_shared<PropertyExpression>(pk.dataType, pk.name, pk.propertyID, expression);
        enumerator->appendScanNodePropIfNecessarySwitch(pkExpression, nodeExpression, plan);
        nodeExpressions.push_back(expression);
        primaryKeyExpressions.push_back(pkExpression);
    }
    auto deleteOperator =
        make_shared<LogicalDelete>(nodeExpressions, primaryKeyExpressions, plan.getLastOperator());
    plan.appendOperator(deleteOperator);
}

} // namespace planner
} // namespace graphflow
