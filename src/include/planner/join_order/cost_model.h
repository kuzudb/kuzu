#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

class CostModel {
public:
    static uint64_t computeExtendCost(const LogicalPlan& childPlan);
    static uint64_t computeRecursiveExtendCost(uint8_t upperBound, double extensionRate,
        const LogicalPlan& childPlan);
    static uint64_t computeHashJoinCost(const std::vector<binder::expression_pair>& joinConditions,
        const LogicalPlan& probe, const LogicalPlan& build);
    static uint64_t computeHashJoinCost(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probe, const LogicalPlan& build);
    static uint64_t computeMarkJoinCost(const std::vector<binder::expression_pair>& joinConditions,
        const LogicalPlan& probe, const LogicalPlan& build);
    static uint64_t computeMarkJoinCost(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probe, const LogicalPlan& build);
    static uint64_t computeIntersectCost(const LogicalPlan& probePlan,
        const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
};

} // namespace planner
} // namespace kuzu
