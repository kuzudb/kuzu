#pragma once

#include "src/processor/include/operator/logical/logical_operator.h"
#include "src/processor/include/plan/physical/physical_plan.h"

namespace graphflow {
namespace processor {

class LogicalPlan {

public:
    LogicalPlan(unique_ptr<LogicalOperator> lastOperator) : lastOperator{move(lastOperator)} {}

    LogicalPlan(const string& path);

    unique_ptr<PhysicalPlan> mapToPhysical(const Graph& graph);

    void serialize(const string& fname);

private:
    unique_ptr<LogicalOperator> lastOperator;
};

} // namespace processor
} // namespace graphflow
