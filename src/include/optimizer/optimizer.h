#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace optimizer {

class Optimizer {
public:
    static void optimize(planner::LogicalPlan* plan);
};

} // namespace optimizer
} // namespace kuzu
