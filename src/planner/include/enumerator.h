#pragma once

#include "src/common/include/configs.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const Catalog& catalog, const BoundSingleQuery& boundSingleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateBoundQueryPart(BoundQueryPart& boundQueryPart);

    void updateQueryGraph(BoundMatchStatement& boundMatchStatement);

    void enumerateSubplans(const vector<shared_ptr<LogicalExpression>>& whereClauseSplitOnAND,
        const vector<shared_ptr<LogicalExpression>>& returnOrWithClause);

    void enumerateSingleQueryNode(
        const vector<shared_ptr<LogicalExpression>>& whereClauseSplitOnAND);

    void enumerateNextLevel(const vector<shared_ptr<LogicalExpression>>& whereClauseSplitOnAND);

    void enumerateHashJoin(const vector<shared_ptr<LogicalExpression>>& whereClauseSplitOnAND);

    void enumerateExtend(const vector<shared_ptr<LogicalExpression>>& whereClauseSplitOnAND);

    void appendLogicalScan(uint32_t queryNodePos, LogicalPlan& plan);

    void appendLogicalExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan);

    void appendLogicalHashJoin(
        uint32_t joinNodePos, const LogicalPlan& planToJoin, LogicalPlan& plan);

    void appendFilter(shared_ptr<LogicalExpression> expression, LogicalPlan& plan);

    void appendProjection(
        const vector<shared_ptr<LogicalExpression>>& returnOrWithClause, LogicalPlan& plan);

    void appendNecessaryScans(shared_ptr<LogicalExpression> expression, LogicalPlan& plan);

    void appendScanNodeProperty(
        const string& nodeName, const string& propertyName, LogicalPlan& plan);

    void appendScanRelProperty(
        const string& relName, const string& propertyName, LogicalPlan& plan);

private:
    unique_ptr<SubgraphPlanTable> subgraphPlanTable; // cached subgraph plans
    const Catalog& catalog;
    const BoundSingleQuery& boundSingleQuery;

    uint32_t currentLevel;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // query rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
};

} // namespace planner
} // namespace graphflow
