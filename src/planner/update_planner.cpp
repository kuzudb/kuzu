#include "include/update_planner.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/planner/logical_plan/logical_operator/include/logical_sink.h"

namespace graphflow {
namespace planner {

void UpdatePlanner::planUpdatingClause(
    BoundUpdatingClause& updatingClause, vector<unique_ptr<LogicalPlan>>& plans) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::SET: {
        planSetClause((BoundSetClause&)updatingClause, plans);
        return;
    }
    case ClauseType::DELETE: {
        assert(false);
    }
    default:
        assert(false);
    }
}

void UpdatePlanner::planSetClause(
    BoundSetClause& setClause, vector<unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        appendSink(*plan);
        appendSet(setClause, *plan);
    }
}

void UpdatePlanner::appendSink(LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto schemaBeforeSink = schema->copy();
    schema->clear();
    Enumerator::computeSchemaForSinkOperators(
        schemaBeforeSink->getGroupsPosInScope(), *schemaBeforeSink, *schema);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = 0u; i < schema->getNumGroups(); ++i) {
        if (schema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto sink = make_shared<LogicalSink>(schemaBeforeSink->getExpressionsInScope(),
        flatOutputGroupPositions, move(schemaBeforeSink), plan.getLastOperator());
    plan.appendOperator(sink);
}

void UpdatePlanner::appendSet(BoundSetClause& setClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> setItems;
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(setItem->target, *schema);
        auto targetPos = Enumerator::appendFlattensButOne(dependentGroupsPos, plan);
        // TODO: xiyang solve together with issue #325
        auto isTargetFlat = targetPos == UINT32_MAX || schema->getGroup(targetPos)->getIsFlat();
        assert(setItem->origin->getChild(0)->dataType.typeID == NODE);
        auto nodeExpression = static_pointer_cast<NodeExpression>(setItem->origin->getChild(0));
        auto originGroupPos = schema->getGroupPos(nodeExpression->getIDProperty());
        auto isOriginFlat = schema->getGroup(originGroupPos)->getIsFlat();
        // If both are unflat and from different groups, we flatten target.
        if (!isTargetFlat && !isOriginFlat && targetPos != originGroupPos) {
            Enumerator::appendFlattenIfNecessary(targetPos, plan);
        }
        setItems.emplace_back(setItem->origin, setItem->target);
    }
    auto set = make_shared<LogicalSet>(move(setItems), plan.getLastOperator());
    plan.appendOperator(set);
}

} // namespace planner
} // namespace graphflow
