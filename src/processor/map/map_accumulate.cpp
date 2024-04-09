#include "planner/operator/logical_accumulate.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapAccumulate(LogicalOperator* op) {
    auto acc = op->constPtrCast<LogicalAccumulate>();
    auto outSchema = acc->getSchema();
    auto inSchema = acc->getChild(0)->getSchema();
    auto prevOperator = mapOperator(acc->getChild(0).get());
    auto expressions = acc->getPayloads();
    auto resultCollector = createResultCollector(acc->getAccumulateType(), expressions, inSchema,
        std::move(prevOperator));
    auto table = resultCollector->getResultFactorizedTable();
    auto maxMorselSize = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    if (acc->hasMark()) {
        expressions.push_back(acc->getMark());
    }
    return createFTableScanAligned(expressions, outSchema, acc->getOffset(), table, maxMorselSize,
        std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
