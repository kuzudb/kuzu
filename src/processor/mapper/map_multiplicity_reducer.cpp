#include "processor/mapper/plan_mapper.h"
#include "processor/operator/multiplicity_reducer.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalMultiplicityReducerToPhysical(
    LogicalOperator* logicalOperator) {
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    return make_unique<MultiplicityReducer>(
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
