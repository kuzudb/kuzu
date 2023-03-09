#pragma once

#include <memory>

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class PhysicalPlan {
public:
    explicit PhysicalPlan(std::unique_ptr<PhysicalOperator> lastOperator)
        : lastOperator{std::move(lastOperator)} {}

    inline bool isCopy() const {
        return lastOperator->getOperatorType() == PhysicalOperatorType::COPY_NODE ||
               lastOperator->getOperatorType() == PhysicalOperatorType::COPY_REL;
    }

public:
    std::unique_ptr<PhysicalOperator> lastOperator;
};

class PhysicalPlanUtil {
public:
    static std::vector<PhysicalOperator*> collectOperators(
        PhysicalOperator* root, PhysicalOperatorType operatorType);

private:
    static void collectOperatorsRecursive(PhysicalOperator* op, PhysicalOperatorType operatorType,
        std::vector<PhysicalOperator*>& result);
};

} // namespace processor
} // namespace kuzu
