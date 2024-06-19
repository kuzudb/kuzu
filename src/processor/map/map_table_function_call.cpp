#include "planner/operator/logical_table_function_call.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapTableFunctionCall(
    LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalTableFunctionCall>();
    std::vector<DataPos> outPosV;
    auto outSchema = call.getSchema();
    for (auto& expr : call.getOutputExpressions()) {
        outPosV.emplace_back(getDataPos(*expr, *outSchema));
    }
    auto info = TableFunctionCallInfo();
    info.function = call.getTableFunc();
    info.bindData = call.getBindData()->copy();
    info.outPosV = outPosV;
    info.rowOffsetPos = getDataPos(*call.getRowIDExpression(), *outSchema);
    info.outputType =
        outPosV.empty() ? TableScanOutputType::EMPTY : TableScanOutputType::SINGLE_DATA_CHUNK;
    auto sharedState = std::make_shared<TableFunctionCallSharedState>();
    auto printInfo = std::make_unique<OPPrintInfo>(call.getExpressionsForPrinting());
    return std::make_unique<TableFunctionCall>(std::move(info), sharedState, getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
