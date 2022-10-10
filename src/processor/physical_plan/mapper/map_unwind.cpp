#include "src/planner/logical_plan/logical_operator/include/logical_unwind.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/unwind.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnwindToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto unwind = (LogicalUnwind*)logicalOperator;
    auto dataPos = mapperContext.getDataPos(unwind->getExpression()->getUniqueName());
    auto expressionEvaluator =
        expressionMapper.mapExpression(unwind->getExpression(), mapperContext);
    mapperContext.addComputedExpressions(unwind->getExpression()->getUniqueName());
    return make_unique<Unwind>(unwind->getExpression(),
        mapperContext.getResultSetDescriptor()->copy(), dataPos, move(expressionEvaluator),
        getOperatorID(), unwind->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
