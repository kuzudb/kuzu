#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

class PlanMapper {

public:
    static unique_ptr<PhysicalPlan> mapToPhysical(
        unique_ptr<LogicalPlan> logicalPlan, const Graph& graph);
};

} // namespace processor
} // namespace graphflow
