#pragma once

#include "src/common/include/profiler.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

using namespace graphflow::planner;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

class PlanPrinter {

public:
    PlanPrinter(unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap)
        : physicalToLogicalOperatorMap{move(physicalToLogicalOperatorMap)} {}

    nlohmann::json printPlanToJson(PhysicalPlan* physicalPlan, Profiler& profiler);

private:
    nlohmann::json toJson(PhysicalOperator* physicalOperator, Profiler& profiler);

private:
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap;
};

} // namespace main
} // namespace graphflow
