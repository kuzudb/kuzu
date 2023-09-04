#pragma once

#include <memory>

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class PhysicalPlan {
public:
    explicit PhysicalPlan(std::unique_ptr<PhysicalOperator> lastOperator)
        : lastOperator{std::move(lastOperator)} {}

public:
    std::unique_ptr<PhysicalOperator> lastOperator;
};

} // namespace processor
} // namespace kuzu
