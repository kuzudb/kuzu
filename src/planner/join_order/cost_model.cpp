#include "planner/join_order/cost_model.h"

#include "common/constants.h"
#include "planner/join_order/join_order_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

uint64_t CostModel::computeExtendCost(const LogicalPlan& childPlan) {
    return childPlan.getCost() + childPlan.estCardinality;
}

uint64_t CostModel::computeRecursiveExtendCost(uint8_t upperBound, double extensionRate,
    const LogicalPlan& childPlan) {
    KU_ASSERT(childPlan.estCardinality >= 1);
    return PlannerKnobs::BUILD_PENALTY * childPlan.estCardinality * (uint64_t)extensionRate *
           upperBound;
}

uint64_t CostModel::computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    KU_ASSERT(probe.getCardinality() >= 1);
    KU_ASSERT(build.getCardinality() >= 1);
    uint64_t cost = 0ul;
    cost += probe.getCost();
    cost += build.getCost();
    cost += probe.getCardinality();
    cost +=
        PlannerKnobs::BUILD_PENALTY * JoinOrderUtil::getJoinKeysFlatCardinality(joinNodeIDs, build);
    return cost;
}

uint64_t CostModel::computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeHashJoinCost(joinNodeIDs, probe, build);
}

uint64_t CostModel::computeIntersectCost(const kuzu::planner::LogicalPlan& probePlan,
    const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    KU_ASSERT(probePlan.getCardinality() >= 1);
    uint64_t cost = 0ul;
    cost += probePlan.getCost();
    // TODO(Xiyang): think of how to calculate intersect cost such that it will be picked in worst
    // case.
    cost += probePlan.getCardinality();
    for (auto& buildPlan : buildPlans) {
        KU_ASSERT(buildPlan->getCardinality() >= 1);
        cost += buildPlan->getCost();
    }
    return cost;
}

} // namespace planner
} // namespace kuzu
