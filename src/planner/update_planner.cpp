#include "include/update_planner.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/logical_operator/include/logical_set.h"

namespace graphflow {
namespace planner {

void UpdatePlanner::planSetClause(
    BoundSetClause& setClause, vector<unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        appendSet(setClause, *plan);
    }
}

void UpdatePlanner::appendSet(BoundSetClause& setClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto schemaBeforeSet = schema->copy();
    schema->clear();
    Enumerator::computeSchemaForSinkOperators(
        schemaBeforeSet->getGroupsPosInScope(), *schemaBeforeSet, *schema);
    vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> setItems;
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(setItem->target, *schema);
        auto targetGroupPos = Enumerator::appendFlattensButOne(dependentGroupsPos, plan);
        // TODO(Xiyang): solve this together with issue #325
        auto isTargetFlat =
            targetGroupPos == UINT32_MAX || schema->getGroup(targetGroupPos)->getIsFlat();
        assert(setItem->origin->getChild(0)->dataType.typeID == NODE);
        auto nodeExpression = static_pointer_cast<NodeExpression>(setItem->origin->getChild(0));
        auto originGroupPos = schema->getGroupPos(nodeExpression->getIDProperty());
        auto isOriginFlat = schema->getGroup(originGroupPos)->getIsFlat();
        // If two unflat vectors are from different groups, we flatten origin
        if (!isTargetFlat && !isOriginFlat && originGroupPos != targetGroupPos) {
            Enumerator::appendFlattenIfNecessary(originGroupPos, plan);
        }
        setItems.emplace_back(setItem->origin, setItem->target);
    }
    auto set =
        make_shared<LogicalSet>(move(setItems), move(schemaBeforeSet), plan.getLastOperator());
    plan.appendOperator(set);
}

} // namespace planner
} // namespace graphflow
