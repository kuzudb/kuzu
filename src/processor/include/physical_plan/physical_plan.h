#pragma once

#include <memory>

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalPlan {

public:
    explicit PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator, bool readOnly)
        : lastOperator{move(lastOperator)}, readOnly{readOnly} {}

    bool isReadOnly() { return readOnly; }

public:
    unique_ptr<PhysicalOperator> lastOperator;
    bool readOnly;
};

} // namespace processor
} // namespace graphflow
