#include "planner/logical_plan/logical_operator/logical_unwind.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/unwind.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnwindToPhysical(
    LogicalOperator* logicalOperator) {
    auto unwind = (LogicalUnwind*)logicalOperator;
    auto outSchema = unwind->getSchema();
    auto inSchema = unwind->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto dataPos = DataPos(outSchema->getExpressionPos(*unwind->getAliasExpression()));
    auto expressionEvaluator = expressionMapper.mapExpression(unwind->getExpression(), *inSchema);
    return std::make_unique<Unwind>(
        *common::VarListType::getChildType(&unwind->getExpression()->dataType), dataPos,
        std::move(expressionEvaluator), std::move(prevOperator), getOperatorID(),
        unwind->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
