#include "binder/expression/literal_expression.h"
#include "planner/operator/logical_standalone_call.h"
#include "processor/operator/call/standalone_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapStandaloneCall(
    planner::LogicalOperator* logicalOperator) {
    auto logicalStandaloneCall = reinterpret_cast<LogicalStandaloneCall*>(logicalOperator);
    auto optionValue =
        reinterpret_cast<binder::LiteralExpression*>(logicalStandaloneCall->getOptionValue().get());
    auto standaloneCallInfo = std::make_unique<StandaloneCallInfo>(
        logicalStandaloneCall->getOption(), *optionValue->getValue());
    return std::make_unique<StandaloneCall>(std::move(standaloneCallInfo),
        PhysicalOperatorType::STANDALONE_CALL, getOperatorID(),
        logicalStandaloneCall->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
