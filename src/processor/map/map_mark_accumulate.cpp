#include "planner/operator/logical_mark_accmulate.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMarkAccumulate(LogicalOperator* op) {
    auto acc = op->constPtrCast<LogicalMarkAccumulate>();
    auto keys = acc->getKeys();
    auto payloads = acc->getPayloads();
    auto outSchema = acc->getSchema();
    auto child = acc->getChild(0).get();
    auto inSchema = child->getSchema();
    auto prevOperator = mapOperator(child);
    return createMarkDistinctHashAggregate(keys, payloads, acc->getMark(), inSchema, outSchema,
        std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
