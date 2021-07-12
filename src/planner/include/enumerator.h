#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/subplans_table.h"
#include "src/storage/include/graph.h"

using namespace graphflow::binder;
using namespace graphflow::storage;

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const Graph& graph, const BoundSingleQuery& boundSingleQuery);

    unique_ptr<LogicalPlan> getBestPlan();

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateBoundQueryPart(BoundQueryPart& boundQueryPart);

    void enumerateBoundReadingStatement(BoundReadingStatement& boundReadingStatement,
        vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void enumerateBoundLoadCSVStatement(const BoundLoadCSVStatement& boundLoadCSVStatement);

    void updateQueryGraphAndWhereClause(BoundMatchStatement& boundMatchStatement,
        vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void enumerateSubplans(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND,
        const vector<shared_ptr<Expression>>& returnOrWithClause);

    void enumerateSingleQueryNode(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void enumerateNextLevel(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void enumerateHashJoin(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void enumerateExtend(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void appendLoadCSV(const BoundLoadCSVStatement& boundLoadCSVStatement, LogicalPlan& plan);

    void appendLogicalScan(uint32_t queryNodePos, LogicalPlan& plan);

    void appendLogicalExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan);

    void appendLogicalHashJoin(
        uint32_t joinNodePos, const LogicalPlan& planToJoin, LogicalPlan& plan);

    void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);

    void appendProjection(
        const vector<shared_ptr<Expression>>& returnOrWithClause, LogicalPlan& plan);

    void appendNecessaryScans(shared_ptr<Expression> expression, LogicalPlan& plan);

    void appendScanNodeProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);

    void appendScanRelProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);

private:
    unique_ptr<SubplansTable> subplansTable; // cached subgraph plans
    const Graph& graph;
    const BoundSingleQuery& boundSingleQuery;

    uint32_t currentLevel;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // query nodes and rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;
};

} // namespace planner
} // namespace graphflow
