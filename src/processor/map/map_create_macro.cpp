#include "planner/operator/logical_create_macro.h"
#include "processor/operator/macro/create_macro.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateMacro(
    planner::LogicalOperator* logicalOperator) {
    auto& logicalCreateMacro = logicalOperator->constCast<LogicalCreateMacro>();
    auto outSchema = logicalCreateMacro.getSchema();
    auto outputExpression = logicalCreateMacro.getOutputExpression();
    auto outputPos = DataPos(outSchema->getExpressionPos(*outputExpression));
    auto createMacroInfo = std::make_unique<CreateMacroInfo>(logicalCreateMacro.getMacroName(),
        logicalCreateMacro.getMacro(), outputPos, clientContext->getCatalog());
    auto printInfo = std::make_unique<CreateMacroPrintInfo>(createMacroInfo->macroName);
    return std::make_unique<CreateMacro>(std::move(createMacroInfo), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
