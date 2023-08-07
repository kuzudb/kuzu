#include "planner/logical_plan/logical_in_query_call.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapInQueryCall(
    planner::LogicalOperator* logicalOperator) {
    auto logicalInQueryCall = reinterpret_cast<LogicalInQueryCall*>(logicalOperator);
    std::vector<DataPos> outputPoses;
    auto outSchema = logicalInQueryCall->getSchema();
    for (auto& outputExpr : logicalInQueryCall->getOutputExpressions()) {
        outputPoses.emplace_back(outSchema->getExpressionPos(*outputExpr));
    }
    auto inQueryCallFuncInfo = std::make_unique<InQueryCallInfo>(logicalInQueryCall->getTableFunc(),
        logicalInQueryCall->getBindData()->copy(), std::move(outputPoses));
    auto inQueryCallSharedState =
        std::make_unique<InQueryCallSharedState>(logicalInQueryCall->getBindData()->maxOffset);
    return std::make_unique<InQueryCall>(std::move(inQueryCallFuncInfo),
        std::move(inQueryCallSharedState), PhysicalOperatorType::IN_QUERY_CALL, getOperatorID(),
        logicalInQueryCall->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
