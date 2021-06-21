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
    PlanPrinter(unique_ptr<PhysicalPlan> physicalPlan,
        unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap)
        : physicalPlan{move(physicalPlan)}, physicalToLogicalOperatorMap{
                                                move(physicalToLogicalOperatorMap)} {}

    nlohmann::json printPlanToJson(Profiler& profiler);

private:
    nlohmann::json toJson(PhysicalOperator* physicalOperator, Profiler& profiler);

private:
    unique_ptr<PhysicalPlan> physicalPlan;
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalToLogicalOperatorMap;
};

} // namespace main
} // namespace graphflow
