#pragma once

#include <memory>
#include <vector>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/query_result.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator) : lastOperator{move(lastOperator)} {}

    PhysicalPlan(const PhysicalPlan& plan) : lastOperator{plan.lastOperator->clone()} {};

public:
    unique_ptr<PhysicalOperator> lastOperator;
};

} // namespace processor
} // namespace graphflow
