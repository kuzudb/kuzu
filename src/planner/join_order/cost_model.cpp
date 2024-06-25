#include "planner/join_order/cost_model.h"

#include "common/constants.h"
#include "planner/join_order/join_order_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

cost_t CostModel::computeExtendCost(cardianlity_t inCardinality) {
    return inCardinality;
}

cost_t CostModel::computeRecursiveExtendCost(cardianlity_t inCardinality, uint8_t upperBound,
    double extensionRate) {
    return PlannerKnobs::BUILD_PENALTY * inCardinality * (uint64_t)extensionRate * upperBound;
}

cost_t CostModel::computeHashJoinCost(cost_t probeCost, cost_t buildCost, cardianlity_t probeCard,
    cardianlity_t buildCard) {
    auto cost = 0u;
    cost += probeCost;
    cost += buildCost;
    cost += probeCard;
    cost += PlannerKnobs::BUILD_PENALTY * buildCard;
    return cost;
}

cost_t CostModel::computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    auto cost = 0ul;
    cost += probe.getCost();
    cost += build.getCost();
    cost += probe.getCardinality();
    cost +=
        PlannerKnobs::BUILD_PENALTY * JoinOrderUtil::getJoinKeysFlatCardinality(joinNodeIDs, build);
    return cost;
}

cost_t CostModel::computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeHashJoinCost(joinNodeIDs, probe, build);
}

cost_t CostModel::computeIntersectCost(kuzu::planner::cost_t probeCost,
    std::vector<cost_t> buildCosts, kuzu::planner::cardianlity_t probeCard) {
    auto cost = 0u;
    cost += probeCost;
    for (auto& buildCost : buildCosts) {
        cost += buildCost;
    }
    cost += probeCard;
    return cost;
}

} // namespace planner
} // namespace kuzu
