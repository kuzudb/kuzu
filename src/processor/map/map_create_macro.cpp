#include "planner/operator/logical_create_macro.h"
#include "processor/operator/macro/create_macro.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateMacro(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCreateMacro = (LogicalCreateMacro*)logicalOperator;
    auto outSchema = logicalCreateMacro->getSchema();
    auto outputExpression = logicalCreateMacro->getOutputExpression();
    auto outputPos = DataPos(outSchema->getExpressionPos(*outputExpression));
    auto createMacroInfo = std::make_unique<CreateMacroInfo>(
        logicalCreateMacro->getMacroName(), logicalCreateMacro->getMacro(), outputPos, catalog);
    return std::make_unique<CreateMacro>(PhysicalOperatorType::CREATE_MACRO,
        std::move(createMacroInfo), getOperatorID(),
        logicalCreateMacro->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
