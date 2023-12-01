#include "planner/operator/logical_unwind.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/unwind.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapUnwind(LogicalOperator* logicalOperator) {
    auto unwind = (LogicalUnwind*)logicalOperator;
    auto outSchema = unwind->getSchema();
    auto inSchema = unwind->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto dataPos = DataPos(outSchema->getExpressionPos(*unwind->getAliasExpression()));
    auto expressionEvaluator = ExpressionMapper::getEvaluator(unwind->getExpression(), inSchema);
    return std::make_unique<Unwind>(*VarListType::getChildType(&unwind->getExpression()->dataType),
        dataPos, std::move(expressionEvaluator), std::move(prevOperator), getOperatorID(),
        unwind->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
