#pragma once

#include "src/planner/include/binder.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const QueryGraph& queryGraph) : queryGraph{queryGraph} {
        subgraphPlanTable = make_unique<SubgraphPlanTable>(queryGraph.numQueryRels());
    };

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateSingleQueryRel(uint32_t& numEnumeratedQueryRels);

    void enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels);

    void enumerateHashJoin(uint32_t nextNumEnumeratedQueryRels);

    void enumerateExtend(uint32_t nextNumEnumeratedQueryRels);

private:
    unique_ptr<SubgraphPlanTable> subgraphPlanTable; // cached subgraph plans
    const QueryGraph& queryGraph;
};

} // namespace planner
} // namespace graphflow
