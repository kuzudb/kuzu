#include "planner/join_order/cost_model.h"

#include "common/constants.h"
#include "planner/join_order/join_order_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

uint64_t CostModel::computeExtendCost(const LogicalPlan& childPlan) {
    return childPlan.getCost() + childPlan.getCardinality();
}

uint64_t CostModel::computeRecursiveExtendCost(uint8_t upperBound, double extensionRate,
    const LogicalPlan& childPlan) {
    return PlannerKnobs::BUILD_PENALTY * childPlan.getCardinality() * (uint64_t)extensionRate *
           upperBound;
}

binder::expression_vector getJoinNodeIDs(
    const std::vector<binder::expression_pair>& joinConditions) {
    binder::expression_vector joinNodeIDs;
    for (auto& [left, _] : joinConditions) {
        if (left->expressionType == ExpressionType::PROPERTY &&
            left->getDataType().getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
            joinNodeIDs.push_back(left);
        }
    }
    return joinNodeIDs;
}

uint64_t CostModel::computeHashJoinCost(const std::vector<binder::expression_pair>& joinConditions,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeHashJoinCost(getJoinNodeIDs(joinConditions), probe, build);
}

uint64_t CostModel::computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    uint64_t cost = 0ul;
    cost += probe.getCost();
    cost += build.getCost();
    cost += probe.getCardinality();
    cost += PlannerKnobs::BUILD_PENALTY *
            JoinOrderUtil::getJoinKeysFlatCardinality(joinNodeIDs, build.getLastOperatorRef());
    return cost;
}

uint64_t CostModel::computeMarkJoinCost(const std::vector<binder::expression_pair>& joinConditions,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeMarkJoinCost(getJoinNodeIDs(joinConditions), probe, build);
}

uint64_t CostModel::computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probe, const LogicalPlan& build) {
    return computeHashJoinCost(joinNodeIDs, probe, build);
}

uint64_t CostModel::computeIntersectCost(const kuzu::planner::LogicalPlan& probePlan,
    const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
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
