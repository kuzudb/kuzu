#include "src/planner/include/subplans_table.h"

#include "src/common/include/assert.h"

namespace graphflow {
namespace planner {

void SubPlansTable::resize(uint32_t newSize) {
    subPlans.resize(newSize + 1);
}

bool SubPlansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    return subPlans[subqueryGraph.queryRelsSelector.count()].contains(subqueryGraph);
}

vector<unique_ptr<LogicalPlan>>& SubPlansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) {
    auto& subqueryGraphPlansMap = subPlans[subqueryGraph.queryRelsSelector.count()];
    GF_ASSERT(subqueryGraphPlansMap.contains(subqueryGraph));
    return subqueryGraphPlansMap.at(subqueryGraph);
}

void SubPlansTable::addPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan) {
    auto& subgraphPlansMap = subPlans[subqueryGraph.queryRelsSelector.count()];
    if (!subgraphPlansMap.contains(subqueryGraph)) {
        subgraphPlansMap.emplace(subqueryGraph, vector<unique_ptr<LogicalPlan>>());
    }
    subgraphPlansMap.at(subqueryGraph).push_back(move(plan));
}

void SubPlansTable::clearUntil(uint32_t size) {
    for (auto i = 0u; i < size; ++i) {
        subPlans[i].clear();
    }
}

} // namespace planner
} // namespace graphflow
