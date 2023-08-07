#include "planner/logical_plan/logical_accumulate.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapAccumulate(LogicalOperator* logicalOperator) {
    auto logicalAccumulate = (LogicalAccumulate*)logicalOperator;
    auto outSchema = logicalAccumulate->getSchema();
    auto inSchema = logicalAccumulate->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalAccumulate->getChild(0).get());
    auto expressions = logicalAccumulate->getExpressions();
    auto resultCollector = createResultCollector(
        logicalAccumulate->getAccumulateType(), expressions, inSchema, std::move(prevOperator));
    auto table = resultCollector->getResultFactorizedTable();
    return createFactorizedTableScan(expressions, outSchema, table, std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
