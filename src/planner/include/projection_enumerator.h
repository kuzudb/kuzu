#pragma once

#include "src/binder/expression/include/rel_expression.h"
#include "src/binder/query/return_with_clause/include/bound_projection_body.h"
#include "src/planner/logical_plan/include/logical_plan.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::storage;
using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator;

class ProjectionEnumerator {
    friend class Enumerator;

public:
    explicit ProjectionEnumerator(const Catalog& catalog, Enumerator* enumerator)
        : catalog{catalog}, enumerator{enumerator} {}

    void enumerateProjectionBody(
        const BoundProjectionBody& projectionBody, const vector<unique_ptr<LogicalPlan>>& plans);

private:
    void enumerateAggregate(const BoundProjectionBody& projectionBody, LogicalPlan& plan);
    void enumerateOrderBy(const BoundProjectionBody& projectionBody, LogicalPlan& plan);
    void enumerateProjection(const BoundProjectionBody& projectionBody, LogicalPlan& plan);
    void enumerateSkipAndLimit(const BoundProjectionBody& projectionBody, LogicalPlan& plan);

    void appendProjection(const expression_vector& expressionsToProject, LogicalPlan& plan);
    void appendDistinct(const expression_vector& expressionsToDistinct, LogicalPlan& plan);
    void appendAggregate(const expression_vector& expressionsToGroupBy,
        const expression_vector& expressionsToAggregate, LogicalPlan& plan);
    void appendOrderBy(
        const expression_vector& expressions, const vector<bool>& isAscOrders, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

    static expression_vector getExpressionToGroupBy(
        const BoundProjectionBody& projectionBody, const Schema& schema);

    static expression_vector getExpressionsToAggregate(
        const BoundProjectionBody& projectionBody, const Schema& schema);

    static expression_vector getExpressionsToProject(
        const BoundProjectionBody& projectionBody, const Schema& schema);

    static expression_vector getSubAggregateExpressionsNotInScope(
        const shared_ptr<Expression>& expression, const Schema& schema);

    static expression_vector rewriteVariableAsAllPropertiesInScope(
        const Expression& variable, const Schema& schema);

private:
    const Catalog& catalog;
    Enumerator* enumerator;
};

} // namespace planner
} // namespace graphflow