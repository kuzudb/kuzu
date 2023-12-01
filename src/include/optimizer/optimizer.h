#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace optimizer {

class Optimizer {
public:
    static void optimize(planner::LogicalPlan* plan, main::ClientContext* context);
};

} // namespace optimizer
} // namespace kuzu
