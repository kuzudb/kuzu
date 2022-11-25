#include "planner/logical_plan/logical_operator/logical_union.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/union_all_scan.h"

using namespace kuzu::planner;

namespace kuzu {
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
    vector<uint32_t> colIndicesToScan;
    auto expressionsToUnion = logicalUnionAll.getExpressionsToUnion();
    for (auto i = 0u; i < expressionsToUnion.size(); ++i) {
        auto expression = expressionsToUnion[i];
        outDataPoses.emplace_back(mapperContext.getDataPos(expression->getUniqueName()));
        outVecDataTypes.push_back(expression->getDataType());
        colIndicesToScan.push_back(i);
    }
    auto unionSharedState =
        make_shared<UnionAllScanSharedState>(std::move(resultCollectorSharedStates));
    return make_unique<UnionAllScan>(mapperContext.getResultSetDescriptor()->copy(),
        std::move(outDataPoses), std::move(outVecDataTypes), std::move(colIndicesToScan),
        unionSharedState, std::move(prevOperators), getOperatorID(),
        logicalUnionAll.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
