#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::planUpdatingClause(
    binder::BoundUpdatingClause& updatingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planUpdatingClause(updatingClause, *plan);
    }
}

void QueryPlanner::planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        auto& createClause = (BoundCreateClause&)updatingClause;
        if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
            expression_vector expressions;
            for (auto& setItem : createClause.getAllSetItems()) {
                expressions.push_back(setItem.second);
            }
            appendExpressionsScan(expressions, plan);
        } else {
            appendAccumulate(common::AccumulateType::REGULAR, plan);
        }
        planCreateClause(updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        appendAccumulate(common::AccumulateType::REGULAR, plan);
        planSetClause(updatingClause, plan);
        return;
    }
    case ClauseType::DELETE_: {
        appendAccumulate(common::AccumulateType::REGULAR, plan);
        planDeleteClause(updatingClause, plan);
        return;
    }
    default:
        throw NotImplementedException("QueryPlanner::planUpdatingClause");
    }
}

void QueryPlanner::planCreateClause(
    binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    auto& createClause = (BoundCreateClause&)updatingClause;
    if (createClause.hasCreateNode()) {
        appendCreateNode(createClause.getCreateNodes(), plan);
    }
    if (createClause.hasCreateRel()) {
        appendCreateRel(createClause.getCreateRels(), plan);
    }
}

void QueryPlanner::planSetClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    auto& setClause = (BoundSetClause&)updatingClause;
    if (setClause.hasSetNodeProperty()) {
        appendSetNodeProperty(setClause.getSetNodeProperties(), plan);
    }
    if (setClause.hasSetRelProperty()) {
        appendSetRelProperty(setClause.getSetRelProperties(), plan);
    }
}

void QueryPlanner::planDeleteClause(
    binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    auto& deleteClause = (BoundDeleteClause&)updatingClause;
    if (deleteClause.hasDeleteRel()) {
        appendDeleteRel(deleteClause.getDeleteRels(), plan);
    }
    if (deleteClause.hasDeleteNode()) {
        appendDeleteNode(deleteClause.getDeleteNodes(), plan);
    }
}

} // namespace planner
} // namespace kuzu
