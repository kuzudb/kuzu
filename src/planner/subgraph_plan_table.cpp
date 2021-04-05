#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

SubgraphPlanTable::SubgraphPlanTable(uint32_t maxSubqueryGraphSize) {
    subgraphPlans.resize(maxSubqueryGraphSize + 1);
}

const vector<shared_ptr<LogicalOperator>>& SubgraphPlanTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) const {
    return subgraphPlans.at(subqueryGraph.queryRelsSelector.count()).at(subqueryGraph);
}

void SubgraphPlanTable::addSubgraphPlan(
    const SubqueryGraph& subQueryGraph, shared_ptr<LogicalOperator> plan) {
    auto& plansWithSameSize = subgraphPlans.at(subQueryGraph.queryRelsSelector.count());
    if (end(plansWithSameSize) == plansWithSameSize.find(subQueryGraph)) {
        plansWithSameSize.emplace(subQueryGraph, vector<shared_ptr<LogicalOperator>>());
    }
    plansWithSameSize.at(subQueryGraph).push_back(plan);
}

} // namespace planner
} // namespace graphflow
