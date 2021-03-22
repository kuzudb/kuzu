#pragma once

#include "src/planner/include/binder.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(unique_ptr<QueryGraph> queryGraph);

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateForInitialQueryRel(const QueryGraph& queryGraph, uint& numQueryRelMatched);

    void enumerateNextQueryRel(const QueryGraph& queryGraph, uint& numQueryRelMatched);

private:
    unique_ptr<SubgraphPlanTable> subgraphPlanTable; // cached subgraph plans
    unique_ptr<QueryGraph> queryGraph;
    uint maxPlanSize;
};

} // namespace planner
} // namespace graphflow
