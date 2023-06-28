#include "binder/expression/literal_expression.h"
#include "planner/logical_plan/logical_operator/logical_call_config.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/call/call_config.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCallConfigToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCallConfig = reinterpret_cast<LogicalCallConfig*>(logicalOperator);
    auto optionValue =
        reinterpret_cast<binder::LiteralExpression*>(logicalCallConfig->getOptionValue().get());
    auto callConfigInfo =
        std::make_unique<CallConfigInfo>(logicalCallConfig->getOption(), *optionValue->getValue());
    return std::make_unique<CallConfig>(std::move(callConfigInfo),
        PhysicalOperatorType::CALL_CONFIG, getOperatorID(),
        logicalCallConfig->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
