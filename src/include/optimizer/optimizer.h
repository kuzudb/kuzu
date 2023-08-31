#pragma once

#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

class Optimizer {
public:
    static void optimize(planner::LogicalPlan* plan);
};

} // namespace optimizer
} // namespace kuzu
