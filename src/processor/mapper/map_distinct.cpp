#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "processor/mapper/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDistinctToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto outSchema = logicalDistinct.getSchema();
    auto inSchema = logicalDistinct.getSchemaBeforeDistinct();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    std::vector<std::unique_ptr<function::AggregateFunction>> emptyAggregateFunctions;
    std::vector<DataPos> emptyInputAggVectorsPos;
    std::vector<DataPos> emptyOutputAggVectorsPos;
    std::vector<DataType> emptyOutputAggVectorsDataTypes;
    return createHashAggregate(std::move(emptyAggregateFunctions), emptyInputAggVectorsPos,
        emptyOutputAggVectorsPos, emptyOutputAggVectorsDataTypes,
        logicalDistinct.getExpressionsToDistinct(), std::move(prevOperator), *inSchema, *outSchema,
        logicalDistinct.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
