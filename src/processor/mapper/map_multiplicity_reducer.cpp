#include "processor/mapper/plan_mapper.h"
#include "processor/operator/multiplicity_reducer.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMultiplicityReducer(
    LogicalOperator* logicalOperator) {
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    return make_unique<MultiplicityReducer>(
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
