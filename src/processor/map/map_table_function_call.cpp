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
    auto columns = call.getColumns();
    auto castedColumns = call.getColumns();
    std::vector<DataPos> columnPos;
    auto outSchema = call.getSchema();
    for (auto& column : columns) {
        columnPos.emplace_back(getDataPos(*column, *outSchema));
    }
    auto info = TableFunctionCallInfo();
    info.function = call.getTableFunc();
    info.bindData = call.getBindData()->copy();
    info.columnPos = columnPos;
    if (call.getOffset() != nullptr) {
        info.rowOffsetPos = getDataPos(*call.getOffset(), *outSchema);
    } else {
        info.rowOffsetPos = DataPos::getInvalidPos();
    }
    info.outputType =
        columnPos.empty() ? TableScanOutputType::EMPTY : TableScanOutputType::SINGLE_DATA_CHUNK;
    if (!castedColumns.empty()) {
        auto expressionMapper = ExpressionMapper(outSchema);
        for (auto i = 0u; i < columns.size(); ++i) {
            if (*columns[i] != *castedColumns[i]) {
                info.castedColumnPos.push_back(getDataPos(*castedColumns[i], *outSchema));
                info.castColumnEvaluator.push_back(expressionMapper.getEvaluator(castedColumns[i]));
            }
        }
    }
    auto sharedState = std::make_shared<TableFunctionCallSharedState>();
    auto printInfo = std::make_unique<TableFunctionCallPrintInfo>(call.getTableFunc().name);
    return std::make_unique<TableFunctionCall>(std::move(info), sharedState, getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
