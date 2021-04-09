#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

SubgraphPlanTable::SubgraphPlanTable(uint32_t maxSubqueryGraphSize) {
    subgraphPlans.resize(maxSubqueryGraphSize + 1);
}

const vector<unique_ptr<LogicalPlan>>& SubgraphPlanTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) const {
    return subgraphPlans.at(subqueryGraph.queryRelsSelector.count()).at(subqueryGraph);
}

void SubgraphPlanTable::addSubgraphPlan(
    const SubqueryGraph& subQueryGraph, unique_ptr<LogicalPlan> plan) {
    auto& plansWithSameSize = subgraphPlans.at(subQueryGraph.queryRelsSelector.count());
    if (end(plansWithSameSize) == plansWithSameSize.find(subQueryGraph)) {
        plansWithSameSize.emplace(subQueryGraph, vector<unique_ptr<LogicalPlan>>());
    }
    plansWithSameSize.at(subQueryGraph).push_back(move(plan));
}

} // namespace planner
} // namespace graphflow
