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
        planCreateClause(updatingClause, plan);
        return;
    }
    case ClauseType::MERGE: {
        throw NotImplementedException("MERGE");
    }
    case ClauseType::SET: {
        planSetClause(updatingClause, plan);
        return;
    }
    case ClauseType::DELETE_: {
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
    if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
        expression_vector expressions;
        for (auto& setItem : createClause.getAllSetItems()) {
            expressions.push_back(setItem.second);
        }
        appendExpressionsScan(expressions, plan);
    } else {
        appendAccumulate(common::AccumulateType::REGULAR, plan);
    }
    if (createClause.hasNodeInfo()) {
        appendCreateNode(createClause.getNodeInfos(), plan);
    }
    if (createClause.hasRelInfo()) {
        appendCreateRel(createClause.getRelInfos(), plan);
    }
}

void QueryPlanner::planSetClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    appendAccumulate(common::AccumulateType::REGULAR, plan);
    auto& setClause = (BoundSetClause&)updatingClause;
    if (setClause.hasNodeInfo()) {
        appendSetNodeProperty(setClause.getNodeInfos(), plan);
    }
    if (setClause.hasRelInfo()) {
        appendSetRelProperty(setClause.getRelInfos(), plan);
    }
}

void QueryPlanner::planDeleteClause(
    binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    appendAccumulate(common::AccumulateType::REGULAR, plan);
    auto& deleteClause = (BoundDeleteClause&)updatingClause;
    if (deleteClause.hasRelInfo()) {
        appendDeleteRel(deleteClause.getRelInfos(), plan);
    }
    if (deleteClause.hasNodeInfo()) {
        appendDeleteNode(deleteClause.getNodeInfos(), plan);
    }
}

} // namespace planner
} // namespace kuzu
