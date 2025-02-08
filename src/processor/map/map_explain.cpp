#include "common/profiler.h"
#include "common/system_config.h"
#include "main/client_context.h"
#include "main/plan_printer.h"
#include "planner/operator/logical_explain.h"
#include "processor/operator/profile.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExplain(const LogicalOperator* logicalOperator) {
    auto& logicalExplain = logicalOperator->constCast<LogicalExplain>();
    auto outSchema = logicalExplain.getSchema();
    auto inSchema = logicalExplain.getChild(0)->getSchema();
    auto lastLogicalOP = logicalExplain.getChild(0);
    auto lastPhysicalOP = mapOperator(lastLogicalOP.get());
    lastPhysicalOP = createResultCollector(AccumulateType::REGULAR,
        logicalExplain.getOutputExpressionsToExplain(), inSchema, std::move(lastPhysicalOP));
    auto outputExpression = logicalExplain.getOutputExpression();
    if (logicalExplain.getExplainType() == ExplainType::PROFILE) {
        auto outputPosition = getDataPos(*outputExpression, *outSchema);
        auto printInfo = std::make_unique<OPPrintInfo>();
        return std::make_unique<Profile>(outputPosition, ProfileInfo{}, ProfileLocalState{},
            getOperatorID(), std::move(lastPhysicalOP), std::move(printInfo));
    }
    if (logicalExplain.getExplainType() == ExplainType::PHYSICAL_PLAN) {
        auto physicalPlanToExplain = std::make_unique<PhysicalPlan>(std::move(lastPhysicalOP));
        auto profiler = std::make_unique<Profiler>();
        auto explainStr =
            main::PlanPrinter::printPlanToOstream(physicalPlanToExplain.get(), profiler.get())
                .str();
        auto factorizedTable = FactorizedTableUtils::getFactorizedTableForOutputMsg(explainStr,
            clientContext->getMemoryManager());
        return createFTableScanAligned(expression_vector{outputExpression}, outSchema,
            factorizedTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */);
    }
    auto logicalPlanToExplain = std::make_unique<LogicalPlan>();
    logicalPlanToExplain->setLastOperator(lastLogicalOP);
    auto explainStr = main::PlanPrinter::printPlanToOstream(logicalPlanToExplain.get()).str();
    auto factorizedTable = FactorizedTableUtils::getFactorizedTableForOutputMsg(explainStr,
        clientContext->getMemoryManager());
    return createFTableScanAligned(expression_vector{outputExpression}, outSchema, factorizedTable,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */);
}

} // namespace processor
} // namespace kuzu
