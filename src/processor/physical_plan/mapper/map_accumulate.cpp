#include "src/planner/logical_plan/logical_operator/include/logical_accumulate.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/factorized_table_scan.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
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
    for (auto& expression : logicalAccumulate->getExpressions()) {
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        outVecDataTypes.push_back(expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
    }
    auto sharedState = resultCollector->getSharedState();
    return make_unique<FactorizedTableScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(outDataPoses), move(outVecDataTypes), sharedState,
        logicalAccumulate->getFlatOutputGroupPositions(), move(resultCollector), getOperatorID(),
        logicalAccumulate->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
