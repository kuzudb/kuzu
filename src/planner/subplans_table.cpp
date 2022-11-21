#include "planner/subplans_table.h"

#include "common/assert.h"

namespace kuzu {
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
    KU_ASSERT(subqueryGraphPlansMap->contains(subqueryGraph));
    return subqueryGraphPlansMap->at(subqueryGraph);
}

vector<SubqueryGraph> SubPlansTable::getSubqueryGraphs(uint32_t level) {
    vector<SubqueryGraph> result;
    for (auto& [subGraph, plans] : *subPlans[level]) {
        result.push_back(subGraph);
    }
    return result;
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
                return a->getCost() < b->getCost();
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
} // namespace kuzu
