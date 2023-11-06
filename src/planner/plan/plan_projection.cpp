#include "binder/expression_visitor.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::planProjectionBody(const BoundProjectionBody& projectionBody,
    const std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        planProjectionBody(projectionBody, *plan);
    }
}

void QueryPlanner::planProjectionBody(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    if (plan.isEmpty()) { // e.g. RETURN 1, COUNT(2)
        appendDummyScan(plan);
    }
    auto expressionsToProject = projectionBody.getProjectionExpressions();
    auto expressionsToAggregate = projectionBody.getAggregateExpressions();
    auto expressionsToGroupBy = projectionBody.getGroupByExpressions();
    if (!expressionsToAggregate.empty()) {
        planAggregate(expressionsToAggregate, expressionsToGroupBy, plan);
    }
    if (projectionBody.hasOrderByExpressions()) {
        planOrderBy(expressionsToProject, projectionBody.getOrderByExpressions(),
            projectionBody.getSortingOrders(), plan);
    }
    appendProjection(expressionsToProject, plan);
    if (projectionBody.getIsDistinct()) {
        appendDistinct(expressionsToProject, plan);
    }
    if (projectionBody.hasSkipOrLimit()) {
        appendMultiplicityReducer(plan);
        appendLimit(projectionBody.getSkipNumber(), projectionBody.getLimitNumber(), plan);
    }
}

void QueryPlanner::planAggregate(const expression_vector& expressionsToAggregate,
    const expression_vector& expressionsToGroupBy, LogicalPlan& plan) {
    KU_ASSERT(!expressionsToAggregate.empty());
    expression_vector expressionsToProject;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        if (ExpressionChildrenCollector::collectChildren(*expressionToAggregate)
                .empty()) { // skip COUNT(*)
            continue;
        }
        expressionsToProject.push_back(expressionToAggregate->getChild(0));
    }
    for (auto& expressionToGroupBy : expressionsToGroupBy) {
        expressionsToProject.push_back(expressionToGroupBy);
    }
    appendProjection(expressionsToProject, plan);
    appendAggregate(expressionsToGroupBy, expressionsToAggregate, plan);
}

void QueryPlanner::planOrderBy(const binder::expression_vector& expressionsToProject,
    const binder::expression_vector& expressionsToOrderBy, const std::vector<bool>& isAscOrders,
    kuzu::planner::LogicalPlan& plan) {
    auto expressionsToProjectBeforeOrderBy = expressionsToProject;
    auto expressionsToProjectSet =
        expression_set{expressionsToProject.begin(), expressionsToProject.end()};
    for (auto& expression : expressionsToOrderBy) {
        if (!expressionsToProjectSet.contains(expression)) {
            expressionsToProjectBeforeOrderBy.push_back(expression);
        }
    }
    appendProjection(expressionsToProjectBeforeOrderBy, plan);
    appendOrderBy(expressionsToOrderBy, isAscOrders, plan);
}

} // namespace planner
} // namespace kuzu
