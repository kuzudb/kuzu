#pragma once

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/join_order_enumerator.h"
#include "src/planner/include/projection_enumerator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator {
    friend class JoinOrderEnumerator;
    friend class ProjectionEnumerator;

public:
    explicit Enumerator(const Graph& graph)
        : joinOrderEnumerator{graph, this}, projectionEnumerator{graph.getCatalog(), this} {}

    unique_ptr<LogicalPlan> getBestJoinOrderPlan(const BoundSingleQuery& singleQuery) {
        return getBestPlan(enumeratePlans(singleQuery));
    }
    // This interface is for testing framework
    vector<unique_ptr<LogicalPlan>> getAllPlans(const BoundSingleQuery& singleQuery) {
        vector<unique_ptr<LogicalPlan>> result;
        for (auto& plan : enumeratePlans(singleQuery)) {
            // This is copy is to avoid sharing operator across plans. Later optimization requires
            // each plan to be independent.
            plan->lastOperator = plan->lastOperator->copy();
            result.push_back(move(plan));
        }
        return result;
    }

private:
    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

    unique_ptr<LogicalPlan> getBestPlan(vector<unique_ptr<LogicalPlan>> plans);

    vector<unique_ptr<LogicalPlan>> enumerateQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

    void planSubquery(
        const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression, LogicalPlan& plan);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    void appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    // return position of the only unFlat group
    // or position of any flat group if there is no unFlat group.
    uint32_t appendFlattensButOne(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);
    void appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan);
    uint32_t appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(
        const shared_ptr<Expression>& expression, LogicalPlan& plan);
    void appendScanPropertiesIfNecessary(
        const shared_ptr<Expression>& expression, LogicalPlan& plan);
    void appendScanNodePropertyIfNecessary(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendScanRelPropertyIfNecessary(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);

    static vector<unique_ptr<LogicalPlan>> getInitialEmptyPlans();
    static vector<shared_ptr<Expression>> getExpressionsInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema);
    static vector<shared_ptr<Expression>> getPropertyExpressionsNotInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema);

private:
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionEnumerator projectionEnumerator;
};

} // namespace planner
} // namespace graphflow
