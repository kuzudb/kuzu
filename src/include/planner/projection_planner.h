#pragma once

#include "binder/query/return_with_clause/bound_projection_body.h"
#include "catalog/catalog.h"
#include "planner/logical_plan/logical_plan.h"

using namespace kuzu::catalog;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class QueryPlanner;

class ProjectionPlanner {
public:
    explicit ProjectionPlanner(QueryPlanner* queryPlanner) : queryPlanner{queryPlanner} {}

    void planProjectionBody(
        const BoundProjectionBody& projectionBody, const vector<unique_ptr<LogicalPlan>>& plans);

private:
    void planProjectionBody(const BoundProjectionBody& projectionBody, LogicalPlan& plan);
    void planAggregate(const expression_vector& expressionsToAggregate,
        const expression_vector& expressionsToGroupBy, LogicalPlan& plan);

    void appendProjection(const expression_vector& expressionsToProject, LogicalPlan& plan);
    void appendAggregate(const expression_vector& expressionsToGroupBy,
        const expression_vector& expressionsToAggregate, LogicalPlan& plan);
    void appendOrderBy(
        const expression_vector& expressions, const vector<bool>& isAscOrders, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

    static expression_vector getExpressionToGroupBy(
        const expression_vector& expressionsToProject, const Schema& schema);
    static expression_vector getExpressionsToAggregate(
        const expression_vector& expressionsToProject, const Schema& schema);

    static expression_vector rewriteExpressionsToProject(
        const expression_vector& expressionsToProject, const Schema& schema);

    static expression_vector getSubAggregateExpressionsNotInScope(
        const shared_ptr<Expression>& expression, const Schema& schema);

    static expression_vector rewriteVariableAsAllPropertiesInScope(
        const Expression& variable, const Schema& schema);

private:
    QueryPlanner* queryPlanner;
};

} // namespace planner
} // namespace kuzu