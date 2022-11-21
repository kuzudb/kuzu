#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/projection.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.getExpressionsToProject()) {
        expressionEvaluators.push_back(expressionMapper.mapExpression(expression, mapperContext));
        expressionsOutputPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    return make_unique<Projection>(move(expressionEvaluators), move(expressionsOutputPos),
        logicalProjection.getDiscardedGroupsPos(), move(prevOperator), getOperatorID(),
        logicalProjection.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
