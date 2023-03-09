#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/flatten.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator) {
    auto flatten = (LogicalFlatten*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    return make_unique<Flatten>(flatten->getGroupPos(), std::move(prevOperator), getOperatorID(),
        flatten->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
