#include "planner/logical_plan/logical_operator/logical_limit.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/limit.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    return make_unique<Limit>(logicalLimit.getLimitNumber(),
        std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos,
        logicalLimit.getGroupsPosToLimit(), std::move(prevOperator), getOperatorID(),
        logicalLimit.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
