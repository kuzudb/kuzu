#include "processor/operator/multiplicity_reducer.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMultiplicityReducer(
    LogicalOperator* logicalOperator) {
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    return make_unique<MultiplicityReducer>(std::move(prevOperator), getOperatorID(),
        logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
