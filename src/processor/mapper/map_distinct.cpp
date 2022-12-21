#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "processor/mapper/plan_mapper.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDistinctToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto outSchema = logicalDistinct.getSchema();
    auto inSchema = logicalDistinct.getSchemaBeforeDistinct();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    vector<unique_ptr<AggregateFunction>> emptyAggregateFunctions;
    vector<DataPos> emptyInputAggVectorsPos;
    vector<DataPos> emptyOutputAggVectorsPos;
    vector<DataType> emptyOutputAggVectorsDataTypes;
    return createHashAggregate(std::move(emptyAggregateFunctions), emptyInputAggVectorsPos,
        emptyOutputAggVectorsPos, emptyOutputAggVectorsDataTypes,
        logicalDistinct.getExpressionsToDistinct(), std::move(prevOperator), *inSchema, *outSchema,
        logicalDistinct.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
