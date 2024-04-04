#include "planner/operator/logical_mark_accmulate.h"
#include "processor/operator/aggregate/aggregate_input.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapMarkAccumulate(LogicalOperator* op) {
    auto logicalMarkAccumulate = ku_dynamic_cast<LogicalOperator*, LogicalMarkAccumulate*>(op);
    auto keys = logicalMarkAccumulate->getKeys();
    auto payloads = logicalMarkAccumulate->getPayloads();
    auto outSchema = logicalMarkAccumulate->getSchema();
    auto inSchema = logicalMarkAccumulate->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalMarkAccumulate->getChild(0).get());
    return createHashAggregate(keys, payloads,
        std::vector<std::unique_ptr<function::AggregateFunction>>{},
        std::vector<std::unique_ptr<AggregateInputInfo>>{}, std::vector<DataPos>{}, inSchema,
        outSchema, std::move(prevOperator), logicalMarkAccumulate->getExpressionsForPrinting(),
        logicalMarkAccumulate->getMark());
}

} // namespace processor
} // namespace kuzu
