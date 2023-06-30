#include "common/profiler.h"
#include "main/plan_printer.h"
#include "planner/logical_plan/logical_operator/logical_explain.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExplainToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExplain = (LogicalExplain*)logicalOperator;
    auto lastPhysicalOP = mapLogicalOperatorToPhysical(logicalExplain->getChild(0));
    auto physicalPlan = std::make_unique<PhysicalPlan>(std::move(lastPhysicalOP));
    auto planPrinter = std::make_unique<main::PlanPrinter>(
        physicalPlan.get(), std::make_unique<common::Profiler>());
    auto planInString = planPrinter->printPlanToOstream().str();
    auto factorizedTable =
        FactorizedTableUtils::getFactorizedTableForOutputMsg(planInString, memoryManager);
    auto ftSharedState = std::make_shared<FTableSharedState>(
        std::move(factorizedTable), common::DEFAULT_VECTOR_CAPACITY);
    auto outSchema = logicalExplain->getSchema();
    auto outputExpression = logicalExplain->getOutputExpression();
    auto outputVectorPos = DataPos(outSchema->getExpressionPos(*outputExpression));
    return std::make_unique<FactorizedTableScan>(std::vector<DataPos>{outputVectorPos},
        std::vector<uint32_t>{0} /* colIndicesToScan */, ftSharedState, getOperatorID(),
        logicalExplain->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
