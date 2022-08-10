#pragma once

#include "join_order_enumerator.h"
#include "projection_enumerator.h"
#include "update_planner.h"

#include "src/binder/bound_copy_csv/include/bound_copy_csv.h"
#include "src/binder/bound_ddl/include/bound_create_node_clause.h"
#include "src/binder/bound_ddl/include/bound_create_rel_clause.h"
#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/binder/expression/include/existential_subquery_expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator {
    friend class JoinOrderEnumerator;
    friend class ProjectionEnumerator;
    friend class UpdatePlanner;

public:
    explicit Enumerator(const Catalog& catalog, const NodesMetadata& nodesMetadata)
        : catalog{catalog}, joinOrderEnumerator{catalog, nodesMetadata, this},
          projectionEnumerator{catalog, this}, updatePlanner{catalog, this} {}

    vector<unique_ptr<LogicalPlan>> getAllPlans(const BoundStatement& boundStatement);

    unique_ptr<LogicalPlan> getBestPlan(const BoundStatement& boundStatement);

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
    static uint32_t appendFlattensButOne(
        const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);

    static void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);

    void appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan);

    // switch structured and unstructured node property scan
    inline void appendScanNodePropIfNecessarySwitch(
        shared_ptr<Expression> property, NodeExpression& node, LogicalPlan& plan) {
        expression_vector properties{move(property)};
        appendScanNodePropIfNecessarySwitch(properties, node, plan);
    }
    void appendScanNodePropIfNecessarySwitch(
        expression_vector& properties, NodeExpression& node, LogicalPlan& plan);
    void appendScanNodePropIfNecessary(
        expression_vector& properties, NodeExpression& node, LogicalPlan& plan, bool isStructured);

    inline void appendScanRelPropsIfNecessary(expression_vector& properties, RelExpression& rel,
        RelDirection direction, LogicalPlan& plan) {
        for (auto& property : properties) {
            appendScanRelPropIfNecessary(property, rel, direction, plan);
        }
    }
    void appendScanRelPropIfNecessary(shared_ptr<Expression>& expression, RelExpression& rel,
        RelDirection direction, LogicalPlan& plan);

    unique_ptr<LogicalPlan> createUnionPlan(
        vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);

    static vector<unique_ptr<LogicalPlan>> getInitialEmptyPlans();

    expression_vector getPropertiesForNode(NodeExpression& node);
    expression_vector getPropertiesForRel(RelExpression& rel);

    static unordered_set<uint32_t> getDependentGroupsPos(
        const shared_ptr<Expression>& expression, const Schema& schema);

    // Recursively walk through expression until the root of current expression tree exists in the
    // schema or expression is a leaf. Collect all such roots.
    static expression_vector getSubExpressionsInSchema(
        const shared_ptr<Expression>& expression, const Schema& schema);

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
    static void computeSchemaForSinkOperators(const unordered_set<uint32_t>& groupsToMaterializePos,
        const Schema& schemaBeforeSink, Schema& schemaAfterSink);

    static vector<vector<unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans);

    static unique_ptr<LogicalPlan> createCreateNodeTablePlan(
        BoundCreateNodeClause& boundCreateNodeClause);

    static unique_ptr<LogicalPlan> createCreateRelTablePlan(
        BoundCreateRelClause& boundCreateRelClause);

    static unique_ptr<LogicalPlan> createCopyCSVPlan(BoundCopyCSV& boundCopyCSV);

private:
    const Catalog& catalog;
    expression_vector propertiesToScan;
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionEnumerator projectionEnumerator;
    UpdatePlanner updatePlanner;
};

} // namespace planner
} // namespace graphflow
