#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/flatten.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto flatten = (LogicalFlatten*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkPos =
        mapperContext.getDataPos(flatten->getExpression()->getUniqueName()).dataChunkPos;
    return make_unique<Flatten>(
        dataChunkPos, move(prevOperator), getOperatorID(), flatten->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
