#include "planner/join_order/cost_model.h"

#include "common/constants.h"

namespace kuzu {
namespace planner {

uint64_t CostModel::computeExtendCost(const LogicalPlan& childPlan) {
    return childPlan.estCardinality;
}

uint64_t CostModel::computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    auto cost = 0u;
    cost += probe.getCost();
    cost += build.getCost();
    cost += probe.getCardinality();
    cost += common::PlannerKnobs::BUILD_PENALTY * build.getCardinality();
    return cost;
}

uint64_t CostModel::computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeHashJoinCost(joinNodeIDs, probe, build);
}

uint64_t CostModel::computeIntersectCost(const kuzu::planner::LogicalPlan& probePlan,
    const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    auto cost = 0u;
    cost += probePlan.getCost();
    // TODO(Xiyang): think of how to calculate intersect cost such that it will be picked in worst
    // case.
    cost += probePlan.getCardinality();
    for (auto& buildPlan : buildPlans) {
        cost += buildPlan->getCost();
    }
    return cost;
}

} // namespace planner
} // namespace kuzu
