#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/flatten.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator) {
    auto flatten = (LogicalFlatten*)logicalOperator;
    auto inSchema = flatten->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto dataChunkPos = inSchema->getExpressionPos(*flatten->getExpression()).first;
    return make_unique<Flatten>(
        dataChunkPos, move(prevOperator), getOperatorID(), flatten->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
