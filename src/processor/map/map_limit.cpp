#include "planner/logical_plan/logical_limit.h"
#include "processor/operator/limit.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLimit(LogicalOperator* logicalOperator) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    return make_unique<Limit>(logicalLimit.getLimitNumber(),
        std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos,
        logicalLimit.getGroupsPosToLimit(), std::move(prevOperator), getOperatorID(),
        logicalLimit.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
