#include "src/planner/logical_plan/logical_operator/include/logical_union.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/union_all_scan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnionAllToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalUnionAll = (LogicalUnion&)*logicalOperator;
    // append result collectors to each child
    vector<unique_ptr<PhysicalOperator>> prevOperators;
    vector<shared_ptr<FTableSharedState>> resultCollectorSharedStates;
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
        auto child = logicalOperator->getChild(i);
        auto childSchema = logicalUnionAll.getSchemaBeforeUnion(i);
        auto childMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*childSchema));
        auto prevOperator = mapLogicalOperatorToPhysical(child, childMapperContext);
        auto resultCollector = appendResultCollector(childSchema->getExpressionsInScope(),
            *childSchema, move(prevOperator), childMapperContext);
        resultCollectorSharedStates.push_back(resultCollector->getSharedState());
        prevOperators.push_back(move(resultCollector));
    }
    // append union all
    vector<DataType> outVecDataTypes;
    vector<DataPos> outDataPoses;
    for (auto& expression : logicalUnionAll.getExpressionsToUnion()) {
        outDataPoses.emplace_back(mapperContext.getDataPos(expression->getUniqueName()));
        outVecDataTypes.push_back(expression->getDataType());
    }
    auto unionSharedState = make_shared<UnionAllScanSharedState>(move(resultCollectorSharedStates));
    return make_unique<UnionAllScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(outDataPoses), move(outVecDataTypes), unionSharedState, move(prevOperators),
        getOperatorID(), logicalUnionAll.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
