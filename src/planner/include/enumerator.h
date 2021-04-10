#pragma once

#include "src/common/include/configs.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const BoundSingleQuery& boundSingleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateSingleQueryNode();

    void enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels);

    void enumerateHashJoin(uint32_t nextNumEnumeratedQueryRels);

    void enumerateExtend(uint32_t nextNumEnumeratedQueryRels);

    void appendLogicalScan(uint32_t queryNodePos, LogicalPlan& plan);

    void appendLogicalExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan);

    void appendLogicalHashJoin(
        uint32_t joinNodePos, const LogicalPlan& planToJoin, LogicalPlan& plan);

    void appendFilter(shared_ptr<LogicalExpression> expression, LogicalPlan& plan);

    void appendProjection(LogicalPlan& plan);

    void appendNecessaryScans(shared_ptr<LogicalExpression> expression, LogicalPlan& plan);

    void appendScanNodeProperty(
        const string& nodeName, const string& propertyName, LogicalPlan& plan);

    void appendScanRelProperty(
        const string& relName, const string& propertyName, LogicalPlan& plan);

private:
    unique_ptr<SubgraphPlanTable> subgraphPlanTable; // cached subgraph plans
    const QueryGraph& queryGraph;
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>
        whereClauseAndIncludedVariables;
    vector<shared_ptr<LogicalExpression>> returnClause;
};

} // namespace planner
} // namespace graphflow
