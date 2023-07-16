#pragma once

#include "binder/query/return_with_clause/bound_projection_body.h"
#include "catalog/catalog.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace planner {

class QueryPlanner;

class ProjectionPlanner {
    friend class JoinOrderEnumerator;

public:
    explicit ProjectionPlanner(QueryPlanner* queryPlanner) : queryPlanner{queryPlanner} {}

    void planProjectionBody(const binder::BoundProjectionBody& projectionBody,
        const std::vector<std::unique_ptr<LogicalPlan>>& plans);

private:
    void planProjectionBody(const binder::BoundProjectionBody& projectionBody, LogicalPlan& plan);
    void planAggregate(const binder::expression_vector& expressionsToAggregate,
        const binder::expression_vector& expressionsToGroupBy, LogicalPlan& plan);
    void planOrderBy(const binder::expression_vector& expressionsToProject,
        const binder::expression_vector& expressionsToOrderBy, const std::vector<bool>& isAscOrders,
        LogicalPlan& plan);

    void appendProjection(const binder::expression_vector& expressionsToProject, LogicalPlan& plan);
    void appendAggregate(const binder::expression_vector& expressionsToGroupBy,
        const binder::expression_vector& expressionsToAggregate, LogicalPlan& plan);
    void appendOrderBy(const binder::expression_vector& expressions,
        const std::vector<bool>& isAscOrders, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

private:
    QueryPlanner* queryPlanner;
};

} // namespace planner
} // namespace kuzu