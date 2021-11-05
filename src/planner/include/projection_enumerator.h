#pragma once

#include "src/binder/include/bound_statements/bound_projection_body.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/rel_expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::storage;
using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator;

class ProjectionEnumerator {

public:
    explicit ProjectionEnumerator(const Catalog& catalog, Enumerator* enumerator)
        : catalog{catalog}, enumerator{enumerator} {}

    void enumerateProjectionBody(const BoundProjectionBody& projectionBody,
        const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn);

private:
    void appendProjection(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan,
        bool isRewritingAllProperties);
    void appendAggregate(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan);
    void appendOrderBy(const vector<shared_ptr<Expression>>& expressions,
        const vector<bool>& isAscOrders, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

    vector<shared_ptr<Expression>> rewriteVariableExpression(
        const shared_ptr<Expression>& variable, bool isRewritingAllProperties);
    vector<shared_ptr<Expression>> rewriteNodeExpression(
        const shared_ptr<NodeExpression>& node, bool isRewritingAllProperties);
    vector<shared_ptr<Expression>> rewriteRelExpression(const shared_ptr<RelExpression>& rel);
    vector<shared_ptr<Expression>> createPropertyExpressions(
        const shared_ptr<Expression>& variable, const vector<PropertyDefinition>& properties);

private:
    const Catalog& catalog;
    Enumerator* enumerator;
};

} // namespace planner
} // namespace graphflow