#include "planner/operator/logical_flatten.h"
#include "processor/operator/flatten.h"
#include "processor/plan_mapper.h"

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
