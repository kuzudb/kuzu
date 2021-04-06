#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

struct SubqueryGraphHasher {
    std::size_t operator()(const SubqueryGraph& key) const {
        return hash<bitset<MAX_NUM_VARIABLES>>{}(key.queryRelsSelector);
    }
};

class SubgraphPlanTable {

public:
    explicit SubgraphPlanTable(uint32_t maxSubqueryGraphSize);

    const vector<shared_ptr<LogicalOperator>>& getSubgraphPlans(
        const SubqueryGraph& subqueryGraph) const;

    void addSubgraphPlan(const SubqueryGraph& subQueryGraph, shared_ptr<LogicalOperator> plan);

public:
    vector<unordered_map<SubqueryGraph, vector<shared_ptr<LogicalOperator>>, SubqueryGraphHasher>>
        subgraphPlans;
};

} // namespace planner
} // namespace graphflow
