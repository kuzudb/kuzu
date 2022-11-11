#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_unwind.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/unwind.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnwindToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto unwind = (LogicalUnwind*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataPos = mapperContext.getDataPos(unwind->getAliasExpression()->getUniqueName());
    auto expressionEvaluator =
        expressionMapper.mapExpression(unwind->getExpression(), mapperContext);
    mapperContext.addComputedExpressions(unwind->getAliasExpression()->getUniqueName());
    return make_unique<Unwind>(*unwind->getExpression()->getDataType().childType, dataPos,
        move(expressionEvaluator), move(prevOperator), getOperatorID(),
        unwind->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
