#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

class CostModel {
public:
    static cost_t computeExtendCost(cardianlity_t inCardinality);
    static cost_t computeRecursiveExtendCost(cardianlity_t inCardinality, uint8_t upperBound,
        double extensionRate);
    static cost_t computeHashJoinCost(cost_t probeCost, cost_t buildCost, cardianlity_t probeCard,
        cardianlity_t buildCard);
    static cost_t computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probe, const LogicalPlan& build);
    static cost_t computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probe, const LogicalPlan& build);
    static cost_t computeIntersectCost(cost_t probeCost, std::vector<cost_t> buildCosts,
        cardianlity_t probeCard);
};

} // namespace planner
} // namespace kuzu
