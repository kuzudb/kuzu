#include "planner/operator/logical_distinct.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDistinct(LogicalOperator* logicalOperator) {
    auto distinct = logicalOperator->constPtrCast<LogicalDistinct>();
    auto child = distinct->getChild(0).get();
    auto outSchema = distinct->getSchema();
    auto inSchema = child->getSchema();
    auto prevOperator = mapOperator(child);
    return createDistinctHashAggregate(distinct->getKeys(), distinct->getPayloads(), inSchema,
        outSchema, std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
