#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/cross_product.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCrossProductToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalCrossProduct = (LogicalCrossProduct*)logicalOperator;
    // map build side
    auto buildSideSchema = logicalCrossProduct->getBuildSideSchema();
    auto buildSideMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*buildSideSchema));
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(logicalCrossProduct->getChild(1), buildSideMapperContext);
    auto resultCollector = appendResultCollector(buildSideSchema->getExpressionsInScope(),
        *buildSideSchema, std::move(buildSidePrevOperator), buildSideMapperContext);
    // map probe side
    auto probeSidePrevOperator =
        mapLogicalOperatorToPhysical(logicalCrossProduct->getChild(0), mapperContext);
    vector<pair<DataPos, DataType>> outVecPosAndTypePairs;
    vector<uint32_t> colIndicesToScan;
    auto expressions = buildSideSchema->getExpressionsInScope();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        auto expressionName = expression->getUniqueName();
        outVecPosAndTypePairs.emplace_back(
            mapperContext.getDataPos(expressionName), expression->dataType);
        mapperContext.addComputedExpressions(expressionName);
        colIndicesToScan.push_back(i);
    }
    return make_unique<CrossProduct>(resultCollector->getSharedState(),
        std::move(outVecPosAndTypePairs), std::move(colIndicesToScan),
        logicalCrossProduct->getFlatOutputGroupPositions(), std::move(probeSidePrevOperator),
        std::move(resultCollector), getOperatorID(),
        logicalCrossProduct->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
