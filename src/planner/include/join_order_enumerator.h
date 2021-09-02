#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/query_normalizer.h"
#include "src/planner/include/subplans_table.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;

namespace graphflow {
namespace planner {

const double PREDICATE_SELECTIVITY = 0.2;

/**
 * JoinOrderEnumerator is currently responsible for
 *      join order enumeration
 *      filter push down
 *      property scanner push down
 */
class JoinOrderEnumerator {

public:
    JoinOrderEnumerator(const Graph& graph)
        : graph{graph}, currentLevel{0}, subPlansTable{make_unique<SubPlansTable>()},
          mergedQueryGraph{make_unique<QueryGraph>()} {};

    vector<unique_ptr<LogicalPlan>> enumerateJoinOrder(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

private:
    void initStatus(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);
    void populatePropertiesMap(const NormalizedQueryPart& queryPart);
    void appendMissingPropertyScans(const vector<unique_ptr<LogicalPlan>>& plans);

    // join order enumeration functions
    void enumerateSingleNode();
    void enumerateHashJoin();
    void enumerateSingleRel();

    // append logical operator functions
    void appendScanNodeID(const NodeExpression& queryNode, LogicalPlan& plan);
    void appendExtendAndFiltersIfNecessary(const RelExpression& queryRel, Direction direction,
        const vector<shared_ptr<Expression>>& expressionsToFilter, LogicalPlan& plan);
    void appendExtend(const RelExpression& queryRel, Direction direction, LogicalPlan& plan);
    void appendLogicalHashJoin(
        const NodeExpression& joinNode, LogicalPlan& buildPlan, LogicalPlan& probePlan);
    // appendIntersect return false if a nodeID is flat in which case we should use filter
    bool appendIntersect(const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan);
    void appendScanPropertiesIfNecessary(
        const string& variableName, bool isNode, LogicalPlan& plan);

    // helper functions
    SubqueryGraph getFullyMatchedSubqueryGraph();
    uint64_t getExtensionRate(label_t boundNodeLabel, label_t relLabel, Direction direction);

private:
    const Graph& graph;

    unordered_map<string, vector<shared_ptr<Expression>>> variableToPropertiesMap;
    vector<shared_ptr<Expression>> whereExpressionsSplitOnAND;

    uint32_t currentLevel;
    unique_ptr<SubPlansTable> subPlansTable;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // We keep track of query nodes and rels matched in previous query graph so that new query part
    // enumeration does not enumerate a rel that exist in previous query parts
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;
};

} // namespace planner
} // namespace graphflow
