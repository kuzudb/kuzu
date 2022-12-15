#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/projection.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto outSchema = logicalProjection.getSchema();
    auto inSchema = logicalProjection.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.getExpressionsToProject()) {
        expressionEvaluators.push_back(expressionMapper.mapExpression(expression, *inSchema));
        expressionsOutputPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    return make_unique<Projection>(std::move(expressionEvaluators), std::move(expressionsOutputPos),
        logicalProjection.getDiscardedGroupsPos(), std::move(prevOperator), getOperatorID(),
        logicalProjection.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
