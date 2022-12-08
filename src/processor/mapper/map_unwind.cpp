#include "planner/logical_plan/logical_operator/logical_unwind.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/unwind.h"

namespace kuzu {
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
} // namespace kuzu
