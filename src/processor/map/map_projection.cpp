#include "planner/operator/logical_projection.h"
#include "processor/operator/projection.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapProjection(LogicalOperator* logicalOperator) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto outSchema = logicalProjection.getSchema();
    auto inSchema = logicalProjection.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> expressionEvaluators;
    std::vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.getExpressionsToProject()) {
        expressionEvaluators.push_back(ExpressionMapper::getEvaluator(expression, inSchema));
        expressionsOutputPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    return make_unique<Projection>(std::move(expressionEvaluators), std::move(expressionsOutputPos),
        logicalProjection.getDiscardedGroupsPos(), std::move(prevOperator), getOperatorID(),
        logicalProjection.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
