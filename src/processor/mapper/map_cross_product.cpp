#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/cross_product.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCrossProductToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalCrossProduct = (LogicalCrossProduct*)logicalOperator;
    auto outSchema = logicalCrossProduct->getSchema();
    // map build side
    auto buildSideSchema = logicalCrossProduct->getBuildSideSchema();
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(logicalCrossProduct->getChild(1));
    auto resultCollector = appendResultCollector(buildSideSchema->getExpressionsInScope(),
        buildSideSchema, std::move(buildSidePrevOperator));
    // map probe side
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(logicalCrossProduct->getChild(0));
    std::vector<DataPos> outVecPos;
    std::vector<uint32_t> colIndicesToScan;
    auto expressions = buildSideSchema->getExpressionsInScope();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        outVecPos.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    auto info =
        std::make_unique<CrossProductInfo>(std::move(outVecPos), std::move(colIndicesToScan));
    auto sharedState = resultCollector->getSharedState();
    auto localState = std::make_unique<CrossProductLocalState>(
        sharedState->getTable(), sharedState->getMaxMorselSize());
    return make_unique<CrossProduct>(std::move(info), std::move(localState),
        std::move(probeSidePrevOperator), std::move(resultCollector), getOperatorID(),
        logicalCrossProduct->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
