#include "src/planner/include/subplans_table.h"

#include "src/common/include/assert.h"

namespace graphflow {
namespace planner {

const uint64_t MAX_NUM_PLANS_PER_SUBGRAPH = 100;

void SubPlansTable::resize(uint32_t newSize) {
    auto prevSize = subPlans.size();
    subPlans.resize(newSize);
    for (auto i = prevSize; i < newSize; ++i) {
        subPlans[i] = make_unique<SubqueryGraphPlansMap>();
    }
}

bool SubPlansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    return subPlans[subqueryGraph.getTotalNumVariables()]->contains(subqueryGraph);
}

vector<unique_ptr<LogicalPlan>>& SubPlansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) {
    auto subqueryGraphPlansMap = subPlans[subqueryGraph.getTotalNumVariables()].get();
    GF_ASSERT(subqueryGraphPlansMap->contains(subqueryGraph));
    return subqueryGraphPlansMap->at(subqueryGraph);
}

void SubPlansTable::addPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan) {
    assert(subPlans[subqueryGraph.getTotalNumVariables()]);
    auto subgraphPlansMap = subPlans[subqueryGraph.getTotalNumVariables()].get();
    if (!subgraphPlansMap->contains(subqueryGraph)) {
        subgraphPlansMap->emplace(subqueryGraph, vector<unique_ptr<LogicalPlan>>());
    }
    subgraphPlansMap->at(subqueryGraph).push_back(move(plan));
}

void SubPlansTable::finalizeLevel(uint32_t level) {
    for (auto& [subgraph, plans] : *subPlans[level]) {
        if (plans.size() < MAX_NUM_PLANS_PER_SUBGRAPH) {
            continue;
        }
        sort(plans.begin(), plans.end(),
            [](const unique_ptr<LogicalPlan>& a, const unique_ptr<LogicalPlan>& b) -> bool {
                return a->cost < b->cost;
            });
        plans.resize(MAX_NUM_PLANS_PER_SUBGRAPH);
    }
}

void SubPlansTable::clear() {
    for (auto& subPlan : subPlans) {
        subPlan->clear();
    }
}

} // namespace planner
} // namespace graphflow
