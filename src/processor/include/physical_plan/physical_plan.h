#pragma once

#include <memory>

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    explicit PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator)
        : lastOperator{move(lastOperator)} {}

    PhysicalPlan(const PhysicalPlan& plan) : lastOperator{plan.lastOperator->clone()} {};

    // TODO (Semih/Xiyang): This will change when CRUD statements/operators are added.
    bool isReadOnly() { return true; }

public:
    unique_ptr<PhysicalOperator> lastOperator;
};

} // namespace processor
} // namespace graphflow
