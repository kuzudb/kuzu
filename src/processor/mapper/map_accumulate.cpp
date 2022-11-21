#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAccumulateToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalAccumulate = (LogicalAccumulate*)logicalOperator;
    // append result collector
    auto child = logicalAccumulate->getChild(0);
    auto childSchema = logicalAccumulate->getSchemaBeforeSink();
    auto childMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*childSchema));
    auto prevOperator = mapLogicalOperatorToPhysical(child, childMapperContext);
    auto resultCollector = appendResultCollector(
        childSchema->getExpressionsInScope(), *childSchema, move(prevOperator), childMapperContext);
    // append factorized table scan
    vector<DataType> outVecDataTypes;
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    auto expressions = logicalAccumulate->getExpressions();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        outVecDataTypes.push_back(expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
        colIndicesToScan.push_back(i);
    }
    auto sharedState = resultCollector->getSharedState();
    return make_unique<FactorizedTableScan>(mapperContext.getResultSetDescriptor()->copy(),
        std::move(outDataPoses), std::move(outVecDataTypes), std::move(colIndicesToScan),
        sharedState, logicalAccumulate->getFlatOutputGroupPositions(), std::move(resultCollector),
        getOperatorID(), logicalAccumulate->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFTableScanToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalFTableScan = (LogicalFTableScan*)logicalOperator;
    vector<DataType> outDataTypes;
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    for (auto& expression : logicalFTableScan->getExpressionsToScan()) {
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        outDataTypes.push_back(expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
        auto colIdx =
            ExpressionUtil::find(expression.get(), logicalFTableScan->getExpressionsAccumulated());
        assert(colIdx != UINT32_MAX);
        colIndicesToScan.push_back(colIdx);
    }
    return make_unique<FactorizedTableScan>(mapperContext.getResultSetDescriptor()->copy(),
        std::move(outDataPoses), std::move(outDataTypes), std::move(colIndicesToScan),
        logicalFTableScan->getFlatOutputGroupPositions(), getOperatorID(),
        logicalFTableScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
