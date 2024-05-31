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
    auto printInfo = std::make_unique<OPPrintInfo>(logicalLimit.getExpressionsForPrinting());
    if (logicalLimit.hasSkipNum()) {
        lastOperator = make_unique<Skip>(logicalLimit.getSkipNum(),
            std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos, groupsPotToLimit,
            std::move(lastOperator), getOperatorID(), printInfo->copy());
    }
    if (logicalLimit.hasLimitNum()) {
        lastOperator = make_unique<Limit>(logicalLimit.getLimitNum(),
            std::make_shared<std::atomic_uint64_t>(0), dataChunkToSelectPos, groupsPotToLimit,
            std::move(lastOperator), getOperatorID(), printInfo->copy());
    }
    return lastOperator;
}

} // namespace processor
} // namespace kuzu
