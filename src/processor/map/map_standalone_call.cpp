#include "binder/expression/literal_expression.h"
#include "planner/operator/logical_standalone_call.h"
#include "processor/operator/standalone_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapStandaloneCall(
    planner::LogicalOperator* logicalOperator) {
    auto logicalStandaloneCall = logicalOperator->constPtrCast<LogicalStandaloneCall>();
    auto optionValue =
        logicalStandaloneCall->getOptionValue()->constPtrCast<binder::LiteralExpression>();
    auto standaloneCallInfo = std::make_unique<StandaloneCallInfo>(
        logicalStandaloneCall->getOption(), optionValue->getValue());
    auto printInfo = std::make_unique<OPPrintInfo>(logicalOperator->getExpressionsForPrinting());
    return std::make_unique<StandaloneCall>(std::move(standaloneCallInfo), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
