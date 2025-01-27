#include "common/exception/runtime.h"
#include "planner/operator/logical_limit.h"
#include "processor/operator/limit.h"
#include "processor/operator/skip.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLimit(const LogicalOperator* logicalOperator) {
    auto& logicalLimit = logicalOperator->constCast<LogicalLimit>();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    auto groupsPotToLimit = logicalLimit.getGroupsPosToLimit();
    std::unique_ptr<PhysicalOperator> lastOperator = std::move(prevOperator);
    if (logicalLimit.hasSkipNum()) {
        if (!logicalLimit.canEvaluateSkipNum()) {
            throw common::RuntimeException{
                "The number of rows to skip is a parameter. Please give a valid skip number."};
        }
        auto skipNum = logicalLimit.evaluateSkipNum();
        auto printInfo = std::make_unique<SkipPrintInfo>(skipNum);
        lastOperator = make_unique<Skip>(skipNum, std::make_shared<std::atomic_uint64_t>(0),
            dataChunkToSelectPos, groupsPotToLimit, std::move(lastOperator), getOperatorID(),
            printInfo->copy());
    }
    if (logicalLimit.hasLimitNum()) {
        if (!logicalLimit.canEvaluateLimitNum()) {
            throw common::RuntimeException{
                "The number of rows to limit is a parameter. Please give a valid limit number."};
        }
        auto limitNum = logicalLimit.evaluateLimitNum();
        auto printInfo = std::make_unique<LimitPrintInfo>(limitNum);
        lastOperator = make_unique<Limit>(limitNum, std::make_shared<std::atomic_uint64_t>(0),
            dataChunkToSelectPos, groupsPotToLimit, std::move(lastOperator), getOperatorID(),
            printInfo->copy());
    }
    return lastOperator;
}

} // namespace processor
} // namespace kuzu
