#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/aggregate/aggregate_input.h"

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
    std::vector<std::unique_ptr<AggregateInputInfo>> emptyAggInputInfos;
    std::vector<DataPos> emptyOutputAggVectorsPos;
    return createHashAggregate(std::move(emptyAggregateFunctions), std::move(emptyAggInputInfos),
        emptyOutputAggVectorsPos, logicalDistinct.getExpressionsToDistinct(),
        std::move(prevOperator), *inSchema, *outSchema,
        logicalDistinct.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
