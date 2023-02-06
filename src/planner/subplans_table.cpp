#include "planner/subplans_table.h"

#include "common/assert.h"

namespace kuzu {
namespace planner {

void SubPlansTable::resize(uint32_t newSize) {
    auto prevSize = subPlans.size();
    subPlans.resize(newSize);
    for (auto i = prevSize; i < newSize; ++i) {
        subPlans[i] = std::make_unique<SubqueryGraphPlansMap>();
    }
}

bool SubPlansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    return subPlans[subqueryGraph.getTotalNumVariables()]->contains(subqueryGraph);
}

std::vector<std::unique_ptr<LogicalPlan>>& SubPlansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) {
    auto subqueryGraphPlansMap = subPlans[subqueryGraph.getTotalNumVariables()].get();
    KU_ASSERT(subqueryGraphPlansMap->contains(subqueryGraph));
    return subqueryGraphPlansMap->at(subqueryGraph);
}

std::vector<SubqueryGraph> SubPlansTable::getSubqueryGraphs(uint32_t level) {
    std::vector<SubqueryGraph> result;
    for (auto& [subGraph, plans] : *subPlans[level]) {
        result.push_back(subGraph);
    }
    return result;
}

void SubPlansTable::addPlan(const SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan) {
    assert(subPlans[subqueryGraph.getTotalNumVariables()]);
    auto subgraphPlansMap = subPlans[subqueryGraph.getTotalNumVariables()].get();
    if (subgraphPlansMap->size() > MAX_NUM_SUBGRAPHS_PER_LEVEL) {
        return;
    }
    if (!subgraphPlansMap->contains(subqueryGraph)) {
        subgraphPlansMap->emplace(subqueryGraph, std::vector<std::unique_ptr<LogicalPlan>>());
    }
    subgraphPlansMap->at(subqueryGraph).push_back(std::move(plan));
}

void SubPlansTable::finalizeLevel(uint32_t level) {
    // cap number of plans per subgraph
    for (auto& [subgraph, plans] : *subPlans[level]) {
        if (plans.size() < MAX_NUM_PLANS_PER_SUBGRAPH) {
            continue;
        }
        sort(plans.begin(), plans.end(),
            [](const std::unique_ptr<LogicalPlan>& a, const std::unique_ptr<LogicalPlan>& b)
                -> bool { return a->getCost() < b->getCost(); });
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
