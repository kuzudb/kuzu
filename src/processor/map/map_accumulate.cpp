#include "planner/operator/logical_accumulate.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapAccumulate(LogicalOperator* logicalOperator) {
    auto logicalAccumulate = (LogicalAccumulate*)logicalOperator;
    auto outSchema = logicalAccumulate->getSchema();
    auto inSchema = logicalAccumulate->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalAccumulate->getChild(0).get());
    auto expressions = logicalAccumulate->getExpressionsToAccumulate();
    auto resultCollector = createResultCollector(
        logicalAccumulate->getAccumulateType(), expressions, inSchema, std::move(prevOperator));
    auto table = resultCollector->getResultFactorizedTable();
    auto maxMorselSize = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    return createFactorizedTableScanAligned(
        expressions, outSchema, table, maxMorselSize, std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
