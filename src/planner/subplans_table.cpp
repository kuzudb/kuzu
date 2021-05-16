#include "src/planner/include/subplans_table.h"

namespace graphflow {
namespace planner {

SubplansTable::SubplansTable(uint32_t maxSubqueryGraphSize) {
    subgraphPlans.resize(maxSubqueryGraphSize + 1);
}

bool SubplansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    auto& subgraphPlansMap = subgraphPlans[subqueryGraph.queryRelsSelector.count()];
    return end(subgraphPlansMap) != subgraphPlansMap.find(subqueryGraph);
}

const vector<unique_ptr<LogicalPlan>>& SubplansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) const {
    return subgraphPlans.at(subqueryGraph.queryRelsSelector.count()).at(subqueryGraph);
}

void SubplansTable::addSubgraphPlan(
    const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan) {
    auto& subgraphPlansMap = subgraphPlans[subqueryGraph.queryRelsSelector.count()];
    if (end(subgraphPlansMap) == subgraphPlansMap.find(subqueryGraph)) {
        subgraphPlansMap.emplace(subqueryGraph, vector<unique_ptr<LogicalPlan>>());
    }
    subgraphPlansMap.at(subqueryGraph).push_back(move(plan));
}

void SubplansTable::clearUntil(uint32_t size) {
    for (auto i = 0u; i < size; ++i) {
        subgraphPlans[i].clear();
    }
}

} // namespace planner
} // namespace graphflow
