#include "common/profiler.h"
#include "main/plan_printer.h"
#include "planner/logical_plan/logical_operator/logical_explain.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/profile.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static DataPos getOutputPos(LogicalExplain* logicalExplain) {
    auto outSchema = logicalExplain->getSchema();
    auto outputExpression = logicalExplain->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExplainToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExplain = (LogicalExplain*)logicalOperator;
    auto lastLogicalOP = logicalExplain->getChild(0);
    auto lastPhysicalOP = mapLogicalOperatorToPhysical(lastLogicalOP);
    lastPhysicalOP = appendResultCollectorIfNotCopy(std::move(lastPhysicalOP),
        logicalExplain->getOutputExpressionsToExplain(), lastLogicalOP->getSchema());
    auto outputVectorPos = getOutputPos(logicalExplain);
    if (logicalExplain->getExplainType() == common::ExplainType::PROFILE) {
        return std::make_unique<Profile>(outputVectorPos, ProfileInfo{}, ProfileLocalState{},
            getOperatorID(), logicalExplain->getExpressionsForPrinting(),
            std::move(lastPhysicalOP));
    } else {
        auto physicalPlanToExplain = std::make_unique<PhysicalPlan>(std::move(lastPhysicalOP));
        auto profiler = std::make_unique<common::Profiler>();
        auto planPrinter =
            std::make_unique<main::PlanPrinter>(physicalPlanToExplain.get(), profiler.get());
        auto explainStr = planPrinter->printPlanToOstream().str();
        auto factorizedTable =
            FactorizedTableUtils::getFactorizedTableForOutputMsg(explainStr, memoryManager);
        auto ftSharedState = std::make_shared<FTableSharedState>(
            std::move(factorizedTable), common::DEFAULT_VECTOR_CAPACITY);
        return std::make_unique<FactorizedTableScan>(std::vector<DataPos>{outputVectorPos},
            std::vector<uint32_t>{0} /* colIndicesToScan */, ftSharedState, getOperatorID(),
            logicalExplain->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
