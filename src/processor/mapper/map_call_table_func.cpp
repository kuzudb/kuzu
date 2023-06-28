#include "planner/logical_plan/logical_operator/logical_call_table_func.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/call/call_table_func.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCallTableFuncToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCallTableFunc = reinterpret_cast<LogicalCallTableFunc*>(logicalOperator);
    std::vector<DataPos> outputPoses;
    auto outSchema = logicalCallTableFunc->getSchema();
    for (auto& outputExpr : logicalCallTableFunc->getOutputExpressions()) {
        outputPoses.emplace_back(outSchema->getExpressionPos(*outputExpr));
    }
    auto callTableFuncInfo =
        std::make_unique<CallTableFuncInfo>(logicalCallTableFunc->getTableFunc(),
            logicalCallTableFunc->getBindData()->copy(), std::move(outputPoses));
    auto callTableFuncSharedState =
        std::make_unique<CallTableFuncSharedState>(logicalCallTableFunc->getBindData()->maxOffset);
    return std::make_unique<CallTableFunc>(std::move(callTableFuncInfo),
        std::move(callTableFuncSharedState), PhysicalOperatorType::CALL_TABLE_FUNC, getOperatorID(),
        logicalCallTableFunc->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
