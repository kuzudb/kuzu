#pragma once

#include "src/planner/include/join_order_enumerator_context.h"
#include "src/planner/include/query_normalizer.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;

namespace graphflow {
namespace planner {

class Enumerator;

const double PREDICATE_SELECTIVITY = 0.2;

/**
 * JoinOrderEnumerator is currently responsible for
 *      join order enumeration
 *      filter push down
 */
class JoinOrderEnumerator {
    friend class Enumerator;

public:
    JoinOrderEnumerator(const Graph& graph, Enumerator* enumerator)
        : graph{graph}, enumerator{enumerator}, context{
                                                    make_unique<JoinOrderEnumeratorContext>()} {};

    vector<unique_ptr<LogicalPlan>> enumerateJoinOrder(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

private:
    unique_ptr<JoinOrderEnumeratorContext> enterSubquery(
        vector<shared_ptr<Expression>> expressionsToSelect);
    void exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext);

    // join order enumeration functions
    void enumerateResultScan();
    void enumerateSingleNode();
    void enumerateHashJoin();
    void enumerateSingleRel();

    // append logical operator functions
    void appendResultScan(const unordered_set<string>& expressionNamesToSelect, LogicalPlan& plan);
    void appendScanNodeID(const NodeExpression& queryNode, LogicalPlan& plan);
    void appendExtendFiltersAndScanPropertiesIfNecessary(const RelExpression& queryRel,
        Direction direction, const vector<shared_ptr<Expression>>& expressionsToFilter,
        LogicalPlan& plan);
    void appendExtend(const RelExpression& queryRel, Direction direction, LogicalPlan& plan);
    void appendLogicalHashJoin(
        const NodeExpression& joinNode, LogicalPlan& buildPlan, LogicalPlan& probePlan);
    // appendIntersect return false if a nodeID is flat in which case we should use filter
    bool appendIntersect(const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan);

    // helper functions
    uint64_t getExtensionRate(label_t boundNodeLabel, label_t relLabel, Direction direction);

private:
    const Graph& graph;
    Enumerator* enumerator;
    unique_ptr<JoinOrderEnumeratorContext> context;
};

} // namespace planner
} // namespace graphflow
