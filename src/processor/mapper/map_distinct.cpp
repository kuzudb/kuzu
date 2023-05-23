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
    auto inSchema = logicalDistinct.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    std::vector<std::unique_ptr<function::AggregateFunction>> emptyAggFunctions;
    std::vector<std::unique_ptr<AggregateInputInfo>> emptyAggInputInfos;
    std::vector<DataPos> emptyAggregatesOutputPos;
    return createHashAggregate(logicalDistinct.getKeyExpressions(),
        logicalDistinct.getDependentKeyExpressions(), std::move(emptyAggFunctions),
        std::move(emptyAggInputInfos), std::move(emptyAggregatesOutputPos), inSchema, outSchema,
        std::move(prevOperator), logicalDistinct.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
