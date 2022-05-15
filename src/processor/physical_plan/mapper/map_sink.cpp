#include "src/planner/logical_plan/logical_operator/include/logical_sink.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/factorized_table_scan.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSinkToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSink = (LogicalSink&)*logicalOperator;
    // append result collector
    auto child = logicalSink.getChild(0);
    auto childSchema = logicalSink.getSchemaBeforeSink();
    auto childMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*childSchema));
    auto prevOperator = mapLogicalOperatorToPhysical(child, childMapperContext);
    auto resultCollector = appendResultCollector(
        childSchema->getExpressionsInScope(), *childSchema, move(prevOperator), childMapperContext);
    // append factorized table scan
    vector<DataType> outVecDataTypes;
    vector<DataPos> outDataPoses;
    for (auto& expression : logicalSink.getExpressions()) {
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        // TODO(Xiyang): solve this together with issue #244
        outVecDataTypes.push_back(expression->getDataType().typeID == NODE_ID ?
                                      DataType(NODE) :
                                      expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
    }
    auto sharedState = resultCollector->getSharedState();
    return make_unique<FactorizedTableScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(outDataPoses), move(outVecDataTypes), sharedState,
        logicalSink.getFlatOutputGroupPositions(), move(resultCollector),
        mapperContext.getOperatorID());
}

} // namespace processor
} // namespace graphflow
