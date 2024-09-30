#include "function/table/call_functions.h"
#include "planner/operator/logical_void_function_call.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapVoidFunctionCall(
    planner::LogicalOperator* logicalOperator) {
    auto call = logicalOperator->constPtrCast<LogicalVoidFunctionCall>();
    auto info = TableFunctionCallInfo();
    info.function = call->getFunc();
    info.bindData = call->getBindData()->copy();
    info.rowOffsetPos = DataPos::getInvalidPos();
    info.outputType = TableScanOutputType::EMPTY;
    auto sharedState = std::make_shared<TableFunctionCallSharedState>();
    auto printInfo = std::make_unique<TableFunctionCallPrintInfo>(info.function.name);
    return std::make_unique<TableFunctionCall>(std::move(info), sharedState, getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
