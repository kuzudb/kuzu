#pragma once

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/join_order_enumerator.h"
#include "src/planner/include/logical_plan/operator/result_collector/logical_result_collector.h"
#include "src/planner/include/logical_plan/operator/union/logical_union.h"
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

    static inline void appendLogicalResultCollector(LogicalPlan& logicalPlan) {
        auto logicalResultCollector =
            make_shared<LogicalResultCollector>(logicalPlan.getExpressionsToCollect(),
                logicalPlan.schema->copy(), logicalPlan.lastOperator);
        logicalPlan.lastOperator = logicalResultCollector;
    }

    static void appendLogicalUnionAndDistinctIfNecessary(
        vector<unique_ptr<LogicalPlan>>& childrenPlans, LogicalPlan& logicalPlan, bool isUnionAll);

    // For HashJoinProbe, the HashJoinProbe operator will read for a particular probe tuple t, the
    // matching result tuples M that match t[k], where k suppose is the join key column. If M
    // consists of flat tuples, i.e., all columns of tuples in M are flat, then we can output the
    // join of t[k] with M as t X M. That is we can put up to M tuples in M into one single unflat
    // dataChunk and output.  Otherwise even if t[k] matches |M| many tuples, because each tuple m
    // in M represents multiple tuples, we need to output the join of t[k] with M one tuple at a
    // time, t X m1, t X m2, etc.
    //
    // For OrderBy a similar logic exists. In order to order a set of tuples R, order by stores R in
    // a FactorizedTable and orders on the keys. The key columns are necessarily flattened (even if
    // they are given to OrderBy in an unflat format). But the non-key columns can be flat or unflat
    // when stored. However, if all of the non-key columns are flat and since all key columns are
    // flattened in FactorizedTable, we can output R with upto |R| many tuples in an unflat
    // datachunk (though we would do it in chunks of DEFAULT_VECTOR_CAPACITY).
    static void computeSchemaForHashJoinAndOrderByAndUnionAll(
        const unordered_set<uint32_t>& groupsToMaterializePos, const Schema& schemaBeforeSink,
        Schema& schemaAfterSink);

private:
    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

    // See logical_plan.h for detailed description of our sub-plan limitation.
    vector<unique_ptr<LogicalPlan>> getValidSubPlans(vector<unique_ptr<LogicalPlan>> plans);
    unique_ptr<LogicalPlan> getBestPlan(vector<unique_ptr<LogicalPlan>> plans);

    vector<unique_ptr<LogicalPlan>> enumerateQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

    void planOptionalMatch(const QueryGraph& queryGraph,
        const shared_ptr<Expression>& queryGraphPredicate, LogicalPlan& outerPlan);

    void planExistsSubquery(const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression,
        LogicalPlan& outerPlan);

    static void appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    // return position of the only unFlat group
    // or position of any flat group if there is no unFlat group.
    uint32_t appendFlattensButOne(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    static void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);
    void appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan);
    void appendScanPropertiesAndPlanSubqueryIfNecessary(
        const shared_ptr<Expression>& expression, LogicalPlan& plan);
    void appendScanPropertiesIfNecessary(
        const shared_ptr<Expression>& expression, LogicalPlan& plan);
    void appendScanNodePropertyIfNecessary(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendScanRelPropertyIfNecessary(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);

    static vector<unique_ptr<LogicalPlan>> getInitialEmptyPlans();
    static unordered_set<uint32_t> getDependentGroupsPos(
        const shared_ptr<Expression>& expression, const Schema& schema);
    // Recursively walk through expression until the root of current expression tree exists in the
    // schema or expression is a leaf. Collect all such roots.
    static vector<shared_ptr<Expression>> getSubExpressionsInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema);
    // Recursively walk through expression, ignoring expression tree whose root exits in the schema,
    // until expression is a leaf. Collect all expressions of given type.
    static vector<shared_ptr<Expression>> getSubExpressionsNotInSchemaOfType(
        const shared_ptr<Expression>& expression, const Schema& schema,
        const std::function<bool(ExpressionType type)>& typeCheckFunc);
    static inline vector<shared_ptr<Expression>> getPropertyExpressionsNotInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema) {
        return getSubExpressionsNotInSchemaOfType(expression, schema,
            [](ExpressionType expressionType) { return expressionType == PROPERTY; });
    }
    static inline vector<shared_ptr<Expression>> getAggregationExpressionsNotInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema) {
        return getSubExpressionsNotInSchemaOfType(expression, schema, isExpressionAggregate);
    }

private:
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionEnumerator projectionEnumerator;
};

} // namespace planner
} // namespace graphflow
