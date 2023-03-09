#include "planner/logical_plan/logical_operator/logical_skip.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/skip.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), std::make_shared<std::atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), std::move(prevOperator),
        getOperatorID(), logicalSkip.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
