#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/query_graph/query_graph.h"

namespace graphflow {
namespace planner {

struct StringUnorderedSetHasher {
    std::size_t operator()(const unordered_set<string>& key) const { return Hash::operation(key); }
};

class SubgraphPlanTable {

public:
    explicit SubgraphPlanTable(uint32_t maxNumQueryRels);

    const unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
        StringUnorderedSetHasher>&
    getSubgraphPlans(uint32_t numQueryRels) const;

    const vector<shared_ptr<LogicalOperator>>& getSubgraphPlans(
        const unordered_set<string>& queryRels) const;

    void addSubgraphPlan(
        const unordered_set<string>& matchedQueryRels, shared_ptr<LogicalOperator> plan);

private:
    void init(uint32_t maxNumQueryRels);

private:
    unordered_map<uint32_t /* num queryRels */,
        unordered_map<unordered_set<string> /* queryRels */, vector<shared_ptr<LogicalOperator>>,
            StringUnorderedSetHasher>>
        subgraphPlans;
};

} // namespace planner
} // namespace graphflow
