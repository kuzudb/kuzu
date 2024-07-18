#include "planner/operator/logical_mark_accmulate.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMarkAccumulate(LogicalOperator* op) {
    auto acc = op->constPtrCast<LogicalMarkAccumulate>();
    auto keys = acc->getKeys();
    auto child = acc->getChild(0).get();
    auto prevOperator = mapOperator(child);
    auto payloads = acc->getPayloads();
    auto outSchema = acc->getSchema();
    auto inSchema = child->getSchema();
    // TODO(ziyi): map this case to a result collector
    if (keys.empty()) {
        auto expressionsToScan = std::move(payloads);
        auto resultCollector = createResultCollector(AccumulateType::EXISTENCE,
            copyVector(expressionsToScan), inSchema, std::move(prevOperator));
        expressionsToScan.push_back(acc->getMark());
        auto ft = resultCollector->getResultFactorizedTable();
        physical_op_vector_t children;
        children.push_back(std::move(resultCollector));
        return createFTableScanAligned(expressionsToScan, outSchema, ft, 2048, std::move(children));
    }
    return createMarkDistinctHashAggregate(keys, payloads, acc->getMark(), inSchema, outSchema,
        std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
