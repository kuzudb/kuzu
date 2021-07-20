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
    explicit Enumerator(const Graph& graph);

    unique_ptr<LogicalPlan> getBestPlan(const BoundSingleQuery& singleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

private:
    void enumerateQueryPart(BoundQueryPart& queryPart);
    vector<unique_ptr<LogicalPlan>>& enumerateProjectionStatement(
        const vector<shared_ptr<Expression>>& expressionsToProject);
    void enumerateReadingStatement(BoundReadingStatement& readingStatement);
    void enumerateLoadCSVStatement(const BoundLoadCSVStatement& loadCSVStatement);
    void updateQueryGraph(QueryGraph& queryGraph);
    void enumerateSubplans(const vector<shared_ptr<Expression>>& whereClause);
    void enumerateSingleQueryNode(const vector<shared_ptr<Expression>>& whereClause);
    void enumerateNextLevel(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);
    void enumerateHashJoin(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);
    void enumerateExtend(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    void appendScan(uint32_t queryNodePos, LogicalPlan& plan);
    void appendExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan);
    string appendNecessaryFlattens(const Expression& expression,
        const unordered_map<uint32_t, string>& unFlatGroupsPos, LogicalPlan& plan);
    void appendFlatten(const string& variable, LogicalPlan& plan);
    void appendLogicalHashJoin(
        uint32_t joinNodePos, LogicalPlan& buildPlan, LogicalPlan& probePlan);
    void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);
    void appendProjection(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan);
    void appendNecessaryScans(const Expression& expression, LogicalPlan& plan);
    void appendScanNodeProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendScanRelProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);

private:
    unique_ptr<SubPlansTable> subPlansTable;
    const Graph& graph;

    uint32_t currentLevel;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // query nodes and rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;
};

} // namespace planner
} // namespace graphflow
