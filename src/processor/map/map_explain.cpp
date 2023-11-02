#include "common/profiler.h"
#include "main/plan_printer.h"
#include "planner/operator/logical_explain.h"
#include "processor/operator/profile.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExplain(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExplain = (LogicalExplain*)logicalOperator;
    auto outSchema = logicalExplain->getSchema();
    auto inSchema = logicalExplain->getChild(0)->getSchema();
    auto lastPhysicalOP = mapOperator(logicalExplain->getChild(0).get());
    lastPhysicalOP = createResultCollector(AccumulateType::REGULAR,
        logicalExplain->getOutputExpressionsToExplain(), inSchema, std::move(lastPhysicalOP));
    auto outputExpression = logicalExplain->getOutputExpression();
    if (logicalExplain->getExplainType() == ExplainType::PROFILE) {
        auto outputPosition = DataPos(outSchema->getExpressionPos(*outputExpression));
        return std::make_unique<Profile>(outputPosition, ProfileInfo{}, ProfileLocalState{},
            getOperatorID(), logicalExplain->getExpressionsForPrinting(),
            std::move(lastPhysicalOP));
    } else {
        auto physicalPlanToExplain = std::make_unique<PhysicalPlan>(std::move(lastPhysicalOP));
        auto profiler = std::make_unique<Profiler>();
        auto planPrinter =
            std::make_unique<main::PlanPrinter>(physicalPlanToExplain.get(), profiler.get());
        auto explainStr = planPrinter->printPlanToOstream().str();
        auto factorizedTable =
            FactorizedTableUtils::getFactorizedTableForOutputMsg(explainStr, memoryManager);
        return createFactorizedTableScanAligned(binder::expression_vector{outputExpression},
            outSchema, factorizedTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, nullptr);
    }
}

} // namespace processor
} // namespace kuzu
