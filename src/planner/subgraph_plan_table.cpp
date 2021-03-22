#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

SubgraphPlanTable::SubgraphPlanTable(uint maxPlanSize) {
    init(maxPlanSize);
}

const unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
    stringUnorderedSetHasher>&
SubgraphPlanTable::getSubgraphPlans(uint planSize) const {
    return subgraphPlans.at(planSize);
}

void SubgraphPlanTable::addSubgraphPlan(
    const unordered_set<string>& matchedQueryRels, shared_ptr<LogicalOperator> plan) {
    auto& subgraphPlansWithSameSize = subgraphPlans.at(matchedQueryRels.size());
    if (end(subgraphPlansWithSameSize) == subgraphPlansWithSameSize.find(matchedQueryRels)) {
        subgraphPlansWithSameSize.emplace(matchedQueryRels, vector<shared_ptr<LogicalOperator>>());
    }
    subgraphPlansWithSameSize.at(matchedQueryRels).push_back(plan);
}

void SubgraphPlanTable::init(uint maxPlanSize) {
    for (auto i = 1u; i <= maxPlanSize; ++i) {
        subgraphPlans.emplace(
            i, unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
                   stringUnorderedSetHasher>());
    }
}

} // namespace planner
} // namespace graphflow
