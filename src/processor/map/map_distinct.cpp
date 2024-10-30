#include "planner/operator/logical_distinct.h"
#include "processor/operator/aggregate/hash_aggregate.h"
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
    uint64_t limitNum = 0;
    if (distinct->hasLimitNum()) {
        limitNum += distinct->getLimitNum();
    }
    if (distinct->hasSkipNum()) {
        limitNum += distinct->getSkipNum();
    }
    if (limitNum == 0) {
        limitNum = UINT64_MAX;
    }
    auto op = createDistinctHashAggregate(distinct->getKeys(), distinct->getPayloads(), inSchema,
        outSchema, std::move(prevOperator));
    auto hashAggregate = op->getChild(0)->ptrCast<HashAggregate>();
    hashAggregate->getSharedState()->setLimitNumber(limitNum);
    auto printInfo = static_cast<const HashAggregatePrintInfo*>(hashAggregate->getPrintInfo());
    const_cast<HashAggregatePrintInfo*>(printInfo)->limitNum = limitNum;
    return op;
}

} // namespace processor
} // namespace kuzu
