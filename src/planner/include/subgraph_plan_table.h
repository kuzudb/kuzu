#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

// hash on node bitset if subgraph has no rel
struct SubqueryGraphHasher {
    std::size_t operator()(const SubqueryGraph& key) const {
        if (0 == key.queryRelsSelector.count()) {
            return hash<bitset<MAX_NUM_VARIABLES>>{}(key.queryNodesSelector);
        }
        return hash<bitset<MAX_NUM_VARIABLES>>{}(key.queryRelsSelector);
    }
};

class SubgraphPlanTable {

public:
    explicit SubgraphPlanTable(uint32_t maxSubqueryGraphSize);

    bool containSubgraphPlans(const SubqueryGraph& subqueryGraph) const;

    const vector<unique_ptr<LogicalPlan>>& getSubgraphPlans(
        const SubqueryGraph& subqueryGraph) const;

    void addSubgraphPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan);

    void clearUntil(uint32_t size);

public:
    vector<unordered_map<SubqueryGraph, vector<unique_ptr<LogicalPlan>>, SubqueryGraphHasher>>
        subgraphPlans;
};

} // namespace planner
} // namespace graphflow
