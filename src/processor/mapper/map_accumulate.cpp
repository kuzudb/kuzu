#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapAccumulate(LogicalOperator* logicalOperator) {
    auto logicalAccumulate = (LogicalAccumulate*)logicalOperator;
    auto outSchema = logicalAccumulate->getSchema();
    auto inSchema = logicalAccumulate->getChild(0)->getSchema();
    // append result collector
    auto prevOperator = mapOperator(logicalAccumulate->getChild(0).get());
    auto expressions = logicalAccumulate->getExpressions();
    auto resultCollector = createResultCollector(expressions, inSchema, std::move(prevOperator));
    // append factorized table scan
    auto table = resultCollector->getResultFactorizedTable();
    return createFactorizedTableScan(expressions, outSchema, table, std::move(resultCollector));
}

} // namespace processor
} // namespace kuzu
