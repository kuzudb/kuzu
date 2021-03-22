#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "src/common/include/operations/hash_operations.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace operation;

namespace graphflow {
namespace planner {

struct stringUnorderedSetHasher {
    std::size_t operator()(const unordered_set<string>& key) const { return Hash::operation(key); }
};

class SubgraphPlanTable {

public:
    explicit SubgraphPlanTable(uint maxPlanSize);

    const unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
        stringUnorderedSetHasher>&
    getSubgraphPlans(uint planSize) const;

    void addSubgraphPlan(
        const unordered_set<string>& matchedQueryRels, shared_ptr<LogicalOperator> plan);

private:
    void init(uint maxPlanSize);

private:
    unordered_map<uint /* plan size */,
        unordered_map<unordered_set<string> /* matched queryRel */,
            vector<shared_ptr<LogicalOperator>>, stringUnorderedSetHasher>>
        subgraphPlans;
};

} // namespace planner
} // namespace graphflow
