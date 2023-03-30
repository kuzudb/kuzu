#include "planner/subplans_table.h"

#include "common/assert.h"

namespace kuzu {
namespace planner {

SubgraphPlans::SubgraphPlans(const kuzu::binder::SubqueryGraph& subqueryGraph) {
    for (auto i = 0u; i < subqueryGraph.queryGraph.getNumQueryNodes(); ++i) {
        if (subqueryGraph.queryNodesSelector[i]) {
            nodeIDsToEncode.push_back(
                subqueryGraph.queryGraph.getQueryNode(i)->getInternalIDProperty());
        }
    }
    maxCost = UINT64_MAX;
}

void SubgraphPlans::addPlan(std::unique_ptr<LogicalPlan> plan) {
    if (plans.size() > MAX_NUM_PLANS) {
        return;
    }
    auto planCode = encodePlan(*plan);
    if (!encodedPlan2PlanIdx.contains(planCode)) {
        encodedPlan2PlanIdx.insert({planCode, plans.size()});
        if (maxCost == UINT64_MAX || plan->getCost() > maxCost) { // update max cost
            maxCost = plan->getCost();
        }
        plans.push_back(std::move(plan));
    } else {
        auto planIdx = encodedPlan2PlanIdx.at(planCode);
        if (plan->getCost() < plans[planIdx]->getCost()) {
            if (plans[planIdx]->getCost() == maxCost) { // update max cost
                maxCost = 0;
                for (auto& plan_ : plans) {
                    if (plan_->getCost() > maxCost) {
                        maxCost = plan_->getCost();
                    }
                }
            }
            plans[planIdx] = std::move(plan);
        }
    }
}

std::bitset<MAX_NUM_QUERY_VARIABLES> SubgraphPlans::encodePlan(const LogicalPlan& plan) {
    auto schema = plan.getSchema();
    std::bitset<MAX_NUM_QUERY_VARIABLES> result;
    result.reset();
    for (auto i = 0; i < nodeIDsToEncode.size(); ++i) {
        result[i] = schema->getGroup(schema->getGroupPos(*nodeIDsToEncode[i]))->isFlat();
    }
    return result;
}

std::vector<SubqueryGraph> DPLevel::getSubqueryGraphs() {
    std::vector<SubqueryGraph> result;
    for (auto& [subGraph, _] : subgraph2Plans) {
        result.push_back(subGraph);
    }
    return result;
}

void DPLevel::addPlan(
    const kuzu::binder::SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan) {
    if (subgraph2Plans.size() > MAX_NUM_SUBGRAPH) {
        return;
    }
    if (!contains(subqueryGraph)) {
        subgraph2Plans.insert({subqueryGraph, std::make_unique<SubgraphPlans>(subqueryGraph)});
    }
    subgraph2Plans.at(subqueryGraph)->addPlan(std::move(plan));
}

void SubPlansTable::resize(uint32_t newSize) {
    auto prevSize = dpLevels.size();
    dpLevels.resize(newSize);
    for (auto i = prevSize; i < newSize; ++i) {
        dpLevels[i] = std::make_unique<DPLevel>();
    }
}

uint64_t SubPlansTable::getMaxCost(const SubqueryGraph& subqueryGraph) const {
    return containSubgraphPlans(subqueryGraph) ?
               getDPLevel(subqueryGraph)->getSubgraphPlans(subqueryGraph)->getMaxCost() :
               UINT64_MAX;
}

bool SubPlansTable::containSubgraphPlans(const SubqueryGraph& subqueryGraph) const {
    return getDPLevel(subqueryGraph)->contains(subqueryGraph);
}

std::vector<std::unique_ptr<LogicalPlan>>& SubPlansTable::getSubgraphPlans(
    const SubqueryGraph& subqueryGraph) {
    auto dpLevel = getDPLevel(subqueryGraph);
    assert(dpLevel->contains(subqueryGraph));
    return dpLevel->getSubgraphPlans(subqueryGraph)->getPlans();
}

std::vector<SubqueryGraph> SubPlansTable::getSubqueryGraphs(uint32_t level) {
    return dpLevels[level]->getSubqueryGraphs();
}

void SubPlansTable::addPlan(const SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan) {
    auto dpLevel = getDPLevel(subqueryGraph);
    dpLevel->addPlan(subqueryGraph, std::move(plan));
}

void SubPlansTable::clear() {
    for (auto& dpLevel : dpLevels) {
        dpLevel->clear();
    }
}

} // namespace planner
} // namespace kuzu
