#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/filter.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto inSchema = logicalFilter.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr = expressionMapper.mapExpression(logicalFilter.expression, *inSchema);
    return make_unique<Filter>(std::move(physicalRootExpr), dataChunkToSelectPos,
        std::move(prevOperator), getOperatorID(), logicalFilter.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
