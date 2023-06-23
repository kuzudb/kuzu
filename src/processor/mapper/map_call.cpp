#include "binder/expression/literal_expression.h"
#include "planner/logical_plan/logical_operator/logical_call.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/call.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCallToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCall = reinterpret_cast<LogicalCall*>(logicalOperator);
    auto optionValue =
        reinterpret_cast<binder::LiteralExpression*>(logicalCall->getOptionValue().get());
    auto callLocalState =
        std::make_unique<CallLocalState>(logicalCall->getOption(), *optionValue->getValue());
    return std::make_unique<Call>(std::move(callLocalState), PhysicalOperatorType::CALL,
        getOperatorID(), logicalCall->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
