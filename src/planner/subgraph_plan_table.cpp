#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

SubgraphPlanTable::SubgraphPlanTable(uint32_t maxNumQueryRels) {
    init(maxNumQueryRels);
}

const unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
    StringUnorderedSetHasher>&
SubgraphPlanTable::getSubgraphPlans(uint32_t numQueryRels) const {
    return subgraphPlans.at(numQueryRels);
}

const vector<shared_ptr<LogicalOperator>>& SubgraphPlanTable::getSubgraphPlans(
    const unordered_set<string>& queryRels) const {
    return subgraphPlans.at(queryRels.size()).at(queryRels);
}

void SubgraphPlanTable::addSubgraphPlan(
    const unordered_set<string>& matchedQueryRels, shared_ptr<LogicalOperator> plan) {
    auto& subgraphPlansWithSameSize = subgraphPlans.at(matchedQueryRels.size());
    if (end(subgraphPlansWithSameSize) == subgraphPlansWithSameSize.find(matchedQueryRels)) {
        subgraphPlansWithSameSize.emplace(matchedQueryRels, vector<shared_ptr<LogicalOperator>>());
    }
    subgraphPlansWithSameSize.at(matchedQueryRels).push_back(plan);
}

void SubgraphPlanTable::init(uint32_t maxPlanSize) {
    for (auto i = 1u; i <= maxPlanSize; ++i) {
        subgraphPlans.emplace(
            i, unordered_map<unordered_set<string>, vector<shared_ptr<LogicalOperator>>,
                   StringUnorderedSetHasher>());
    }
}

} // namespace planner
} // namespace graphflow
