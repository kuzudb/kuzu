#include "processor/physical_plan.h"

namespace kuzu {
namespace processor {

std::vector<PhysicalOperator*> PhysicalPlanUtil::collectOperators(
    PhysicalOperator* root, PhysicalOperatorType operatorType) {
    std::vector<PhysicalOperator*> result;
    collectOperatorsRecursive(root, operatorType, result);
    return result;
}

void PhysicalPlanUtil::collectOperatorsRecursive(PhysicalOperator* op,
    PhysicalOperatorType operatorType, std::vector<PhysicalOperator*>& result) {
    if (op->getOperatorType() == operatorType) {
        result.push_back(op);
    }
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        collectOperatorsRecursive(op->getChild(i), operatorType, result);
    }
}

} // namespace processor
} // namespace kuzu
