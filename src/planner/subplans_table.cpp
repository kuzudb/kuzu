#include "planner/subplans_table.h"

#include "common/assert.h"

namespace kuzu {
namespace planner {

void SubPlansTable::resize(uint32_t newSize) {
    auto prevSize = dpLevels.size();
    dpLevels.resize(newSize);
    for (auto i = prevSize; i < newSize; ++i) {
        dpLevels[i] = std::make_unique<dp_level_t>();
    }
}

bool SubPlansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    return getDPLevel(subqueryGraph)->contains(subqueryGraph);
}

std::vector<std::unique_ptr<LogicalPlan>>& SubPlansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) {
    auto dpLevel = getDPLevel(subqueryGraph);
    KU_ASSERT(dpLevel->contains(subqueryGraph));
    return dpLevel->at(subqueryGraph)->getPlans();
}

std::vector<SubqueryGraph> SubPlansTable::getSubqueryGraphs(uint32_t level) {
    std::vector<SubqueryGraph> result;
    for (auto& [subGraph, _] : *dpLevels[level]) {
        result.push_back(subGraph);
    }
    return result;
}

void SubPlansTable::addPlan(const SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan) {
    auto dpLevel = getDPLevel(subqueryGraph);
    if (dpLevel->size() > MAX_NUM_SUBGRAPHS_PER_LEVEL) {
        return;
    }
    if (!dpLevel->contains(subqueryGraph)) {
        dpLevel->emplace(subqueryGraph, std::make_unique<PlanSet>());
    }
    dpLevel->at(subqueryGraph)->addPlan(std::move(plan));
}

void SubPlansTable::clear() {
    for (auto& dpLevel : dpLevels) {
        dpLevel->clear();
    }
}

void SubPlansTable::PlanSet::addPlan(std::unique_ptr<LogicalPlan> plan) {
    if (plans.size() >= MAX_NUM_PLANS_PER_SUBGRAPH) {
        return;
    }
    auto schema = plan->getSchema();
    if (!schemaToPlanIdx.contains(schema)) { // add plan if this is a new factorization schema
        schemaToPlanIdx.insert({schema, plans.size()});
        plans.push_back(std::move(plan));
    } else { // swap plan for lower cost under the same factorization schema
        auto idx = schemaToPlanIdx.at(schema);
        assert(idx < MAX_NUM_PLANS_PER_SUBGRAPH);
        auto currentPlan = plans[idx].get();
        if (currentPlan->getCost() > plan->getCost()) {
            plans[idx] = std::move(plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
