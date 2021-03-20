#pragma once

#include <memory>
#include <vector>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/plan_output.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator) : lastOperator{move(lastOperator)} {}

    PhysicalPlan(const PhysicalPlan& plan) : lastOperator{plan.lastOperator->clone()} {};

    void run();

    unique_ptr<PlanOutput> getPlanOutput() {
        return make_unique<PlanOutput>(((Sink*)lastOperator.get())->getNumTuples());
    }

private:
    unique_ptr<PhysicalOperator> lastOperator;
};

} // namespace processor
} // namespace graphflow
