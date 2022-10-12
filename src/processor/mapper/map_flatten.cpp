#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_flatten.h"
#include "src/processor/operator/include/flatten.h"

namespace graphflow {
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
} // namespace graphflow
