#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

SubgraphPlanTable::SubgraphPlanTable(uint32_t maxSubqueryGraphSize) {
    subgraphPlans.resize(maxSubqueryGraphSize + 1);
}

bool SubgraphPlanTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    auto& subgraphPlansMap = subgraphPlans[subqueryGraph.queryRelsSelector.count()];
    return end(subgraphPlansMap) != subgraphPlansMap.find(subqueryGraph);
}

const vector<unique_ptr<LogicalPlan>>& SubgraphPlanTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) const {
    return subgraphPlans.at(subqueryGraph.queryRelsSelector.count()).at(subqueryGraph);
}

void SubgraphPlanTable::addSubgraphPlan(
    const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan) {
    auto& subgraphPlansMap = subgraphPlans[subqueryGraph.queryRelsSelector.count()];
    if (end(subgraphPlansMap) == subgraphPlansMap.find(subqueryGraph)) {
        subgraphPlansMap.emplace(subqueryGraph, vector<unique_ptr<LogicalPlan>>());
    }
    subgraphPlansMap.at(subqueryGraph).push_back(move(plan));
}

void SubgraphPlanTable::clearUntil(uint32_t size) {
    for (auto i = 0u; i < size; ++i) {
        subgraphPlans[i].clear();
    }
}

} // namespace planner
} // namespace graphflow
