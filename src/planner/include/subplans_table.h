#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "src/binder/query/match_clause/include/query_graph.h"
#include "src/planner/logical_plan/include/logical_plan.h"

using namespace graphflow::binder;

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

typedef unordered_map<SubqueryGraph, vector<unique_ptr<LogicalPlan>>, SubqueryGraphHasher>
    SubqueryGraphPlansMap;

class SubPlansTable {

public:
    void resize(uint32_t newSize);

    bool containSubgraphPlans(const SubqueryGraph& subqueryGraph) const;

    vector<unique_ptr<LogicalPlan>>& getSubgraphPlans(const SubqueryGraph& subqueryGraph);

    SubqueryGraphPlansMap* getSubqueryGraphPlansMap(uint32_t level) {
        return subPlans[level].get();
    }

    void addPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan);

    void clear();

private:
    vector<unique_ptr<SubqueryGraphPlansMap>> subPlans;
};

} // namespace planner
} // namespace graphflow
