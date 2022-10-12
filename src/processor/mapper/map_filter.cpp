#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_filter.h"
#include "src/processor/operator/include/filter.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr = expressionMapper.mapExpression(logicalFilter.expression, mapperContext);
    return make_unique<Filter>(move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator),
        getOperatorID(), logicalFilter.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
