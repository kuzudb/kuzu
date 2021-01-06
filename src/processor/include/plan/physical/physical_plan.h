#pragma once

#include <memory>
#include <vector>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/physical/operator.h"
#include "src/processor/include/operator/physical/sink/sink.h"
#include "src/processor/include/plan/physical/plan_output.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    PhysicalPlan(unique_ptr<Operator> lastOperator) : lastOperator{move(lastOperator)} {}

    PhysicalPlan(const PhysicalPlan& plan) : lastOperator{move(plan.lastOperator->clone())} {};

    void run();

    unique_ptr<PlanOutput> getPlanOutput() {
        return make_unique<PlanOutput>(((Sink*)lastOperator.get())->getNumTuples());
    }

private:
    unique_ptr<Operator> lastOperator;
};

} // namespace processor
} // namespace graphflow
