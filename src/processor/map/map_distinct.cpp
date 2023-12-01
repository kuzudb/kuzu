#include "planner/operator/logical_distinct.h"
#include "processor/operator/aggregate/aggregate_input.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDistinct(LogicalOperator* logicalOperator) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto outSchema = logicalDistinct.getSchema();
    auto inSchema = logicalDistinct.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
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
