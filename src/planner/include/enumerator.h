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
    explicit Enumerator(const Graph& graph)
        : graph{graph}, currentLevel{0}, subPlansTable{make_unique<SubPlansTable>()},
          mergedQueryGraph{make_unique<QueryGraph>()} {}

    unique_ptr<LogicalPlan> getBestPlan(const BoundSingleQuery& singleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

private:
    void enumerateQueryPart(BoundQueryPart& queryPart);
    void enumerateProjectionBody(const BoundProjectionBody& projectionBody, bool isFinalReturn);
    void enumerateReadingStatement(BoundReadingStatement& readingStatement);
    void enumerateLoadCSVStatement(const BoundLoadCSVStatement& loadCSVStatement);
    void updateQueryGraph(QueryGraph& queryGraph);
    void enumerateSubplans(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateSingleNode(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateNextLevel(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateHashJoin(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateSingleRel(const vector<shared_ptr<Expression>>& whereExpressions);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    void appendScanNodeID(const NodeExpression& queryNode, LogicalPlan& plan);
    void appendExtendAndFiltersIfNecessary(const RelExpression& queryRel, Direction direction,
        const vector<shared_ptr<Expression>>& expressionsToFilter, LogicalPlan& plan);
    void appendExtend(const RelExpression& queryRel, Direction direction, LogicalPlan& plan);
    uint32_t appendFlattensIfNecessary(
        const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan);
    void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);
    void appendLogicalHashJoin(
        const NodeExpression& joinNode, LogicalPlan& buildPlan, LogicalPlan& probePlan);
    void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);
    bool appendIntersect(const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan);
    void appendProjection(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan,
        bool isRewritingAllProperties);
    void appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan);
    void appendScanPropertiesIfNecessary(
        const string& variableName, bool isNode, LogicalPlan& plan);
    void appendScanNodeProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendScanRelProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

    void populatePropertiesMapAndAppendPropertyScansIfNecessary(
        const vector<shared_ptr<Expression>>& propertyExpressions);

private:
    const Graph& graph;
    unordered_map<string, vector<shared_ptr<Expression>>>
        variableToPropertiesMapForCurrentQueryPart;

    uint32_t currentLevel;
    unique_ptr<SubPlansTable> subPlansTable;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // query nodes and rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;
};

} // namespace planner
} // namespace graphflow
