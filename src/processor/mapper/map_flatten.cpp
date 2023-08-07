#include "planner/logical_plan/logical_flatten.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/flatten.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapFlatten(LogicalOperator* logicalOperator) {
    auto flatten = (LogicalFlatten*)logicalOperator;
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    return make_unique<Flatten>(flatten->getGroupPos(), std::move(prevOperator), getOperatorID(),
        flatten->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
