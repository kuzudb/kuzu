#include "src/planner/include/subplans_table.h"

#include "src/common/include/assert.h"

namespace graphflow {
namespace planner {

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

void SubPlansTable::clear() {
    for (auto& subPlan : subPlans) {
        subPlan->clear();
    }
}

} // namespace planner
} // namespace graphflow
