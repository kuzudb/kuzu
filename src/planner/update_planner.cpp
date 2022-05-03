#include "include/update_planner.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/logical_operator/include/logical_set.h"

namespace graphflow {
namespace planner {

void UpdatePlanner::planSetClause(
    BoundSetClause& setClause, vector<unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        appendLogicalSet(setClause, *plan);
    }
}

void UpdatePlanner::appendLogicalSet(BoundSetClause& setClause, LogicalPlan& plan) {
    vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> setItems;
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto dependentGroupsPos =
            Enumerator::getDependentGroupsPos(setItem->target, *plan.getSchema());
        Enumerator::appendFlattensButOne(dependentGroupsPos, plan);
        setItems.emplace_back(setItem->origin, setItem->target);
    }
    auto schemaBeforeSet = plan.getSchema()->copy();
    plan.getSchema()->clear();
    Enumerator::computeSchemaForSinkOperators(
        schemaBeforeSet->getGroupsPosInScope(), *schemaBeforeSet, *plan.getSchema());
    auto set =
        make_shared<LogicalSet>(move(setItems), move(schemaBeforeSet), plan.getLastOperator());
    plan.appendOperator(set);
}

} // namespace planner
} // namespace graphflow
