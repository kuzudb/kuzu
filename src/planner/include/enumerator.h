#pragma once

#include "enumerator_utils.h"

#include "src/binder/expression/include/existential_subquery_expression.h"
#include "src/binder/query/include/bound_regular_query.h"
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

    vector<unique_ptr<LogicalPlan>> getAllPlans(const BoundRegularQuery& regularQuery);

    unique_ptr<LogicalPlan> getBestPlan(const BoundRegularQuery& regularQuery);

private:
    // See logical_plan.h for detailed description of our sub-plan limitation.
    vector<unique_ptr<LogicalPlan>> getValidSubPlans(vector<unique_ptr<LogicalPlan>> plans);

    unique_ptr<LogicalPlan> getBestPlan(vector<unique_ptr<LogicalPlan>> plans);

    vector<unique_ptr<LogicalPlan>> getAllPlans(const NormalizedSingleQuery& singleQuery);

    unique_ptr<LogicalPlan> getBestPlan(const NormalizedSingleQuery& singleQuery) {
        return getBestPlan(enumeratePlans(singleQuery));
    }

    vector<unique_ptr<LogicalPlan>> enumeratePlans(const NormalizedSingleQuery& singleQuery);

    vector<unique_ptr<LogicalPlan>> enumerateQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

    void planOptionalMatch(const QueryGraph& queryGraph,
        const shared_ptr<Expression>& queryGraphPredicate, LogicalPlan& outerPlan);

    void planExistsSubquery(const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression,
        LogicalPlan& outerPlan);

    void planSubqueryIfNecessary(const shared_ptr<Expression>& expression, LogicalPlan& plan);

    static void appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);

    // return position of the only unFlat group
    // or position of any flat group if there is no unFlat group.
    uint32_t appendFlattensButOne(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);

    static void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);

    void appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan);

    void appendScanNodeProperty(const NodeExpression& node, LogicalPlan& plan);

    void appendScanNodeProperty(
        const property_vector& properties, const NodeExpression& node, LogicalPlan& plan);

    void appendScanRelProperty(const RelExpression& rel, LogicalPlan& plan);

    void appendScanRelProperty(const shared_ptr<PropertyExpression>& property,
        const RelExpression& rel, LogicalPlan& plan);

    static void appendResultCollector(LogicalPlan& plan);

    unique_ptr<LogicalPlan> createUnionPlan(
        vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);

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
    static inline vector<shared_ptr<Expression>> getAggregationExpressionsNotInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema) {
        return getSubExpressionsNotInSchemaOfType(expression, schema, isExpressionAggregate);
    }

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
    static void computeSchemaForHashJoinOrderByAndUnion(
        const unordered_set<uint32_t>& groupsToMaterializePos, const Schema& schemaBeforeSink,
        Schema& schemaAfterSink);

    static vector<vector<unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans);

private:
    property_vector propertiesToScan;
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionEnumerator projectionEnumerator;
};

} // namespace planner
} // namespace graphflow
