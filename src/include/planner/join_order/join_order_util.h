#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

struct JoinOrderUtil {
    // Although we do not flatten join key in Build operator computation. We still need to perform
    // cardinality and cost estimation based on their flat cardinality.
    static uint64_t getJoinKeysFlatCardinality(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& buildPlan);
};

} // namespace planner
} // namespace kuzu
