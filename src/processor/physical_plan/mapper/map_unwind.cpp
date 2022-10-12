#include "src/planner/logical_plan/logical_operator/include/logical_unwind.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/unwind.h"

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
    auto exprReturnDataType = make_shared<DataType>(unwind->getExpression()->getDataType());
    return make_unique<Unwind>(exprReturnDataType, dataPos, move(expressionEvaluator),
        move(prevOperator), getOperatorID(), unwind->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
