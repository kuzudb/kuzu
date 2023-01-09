#pragma once

#include <memory>

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class PhysicalPlan {
public:
    explicit PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator, bool isDDLOrCopyCSV)
        : lastOperator{std::move(lastOperator)}, isDDLOrCopyCSV{isDDLOrCopyCSV} {}

public:
    unique_ptr<PhysicalOperator> lastOperator;
    bool isDDLOrCopyCSV;
};

class PhysicalPlanUtil {
public:
    static vector<PhysicalOperator*> collectOperators(
        PhysicalOperator* root, PhysicalOperatorType operatorType);

private:
    static void collectOperatorsRecursive(
        PhysicalOperator* op, PhysicalOperatorType operatorType, vector<PhysicalOperator*>& result);
};

} // namespace processor
} // namespace kuzu
