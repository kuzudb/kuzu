#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "processor/mapper/plan_mapper.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDistinctToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto mapperContextBeforeDistinct =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalDistinct.getSchemaBeforeDistinct()));
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContextBeforeDistinct);
    vector<unique_ptr<AggregateFunction>> emptyAggregateFunctions;
    vector<DataPos> emptyInputAggVectorsPos;
    vector<DataPos> emptyOutputAggVectorsPos;
    vector<DataType> emptyOutputAggVectorsDataTypes;
    return createHashAggregate(move(emptyAggregateFunctions), emptyInputAggVectorsPos,
        emptyOutputAggVectorsPos, emptyOutputAggVectorsDataTypes,
        logicalDistinct.getExpressionsToDistinct(), logicalDistinct.getSchemaBeforeDistinct(),
        move(prevOperator), mapperContextBeforeDistinct, mapperContext,
        logicalDistinct.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
