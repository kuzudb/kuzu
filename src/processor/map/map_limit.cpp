#include "planner/operator/logical_limit.h"
#include "processor/operator/limit.h"
#include "processor/operator/skip.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLimit(LogicalOperator* logicalOperator) {
    auto& logicalLimit = logicalOperator->constCast<LogicalLimit>();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    auto groupsPotToLimit = logicalLimit.getGroupsPosToLimit();
    std::unique_ptr<PhysicalOperator> lastOperator = std::move(prevOperator);
    if (logicalLimit.hasSkipNum()) {
        auto printInfo = std::make_unique<SkipPrintInfo>(logicalLimit.getSkipNum());
        lastOperator = make_unique<Skip>(logicalLimit.getSkipNum(),
            std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos, groupsPotToLimit,
            std::move(lastOperator), getOperatorID(), printInfo->copy());
    }
    if (logicalLimit.hasLimitNum()) {
        auto printInfo = std::make_unique<LimitPrintInfo>(logicalLimit.getLimitNum());
        lastOperator = make_unique<Limit>(logicalLimit.getLimitNum(),
            std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos, groupsPotToLimit,
            std::move(lastOperator), getOperatorID(), printInfo->copy());
    }
    return lastOperator;
}

} // namespace processor
} // namespace kuzu
