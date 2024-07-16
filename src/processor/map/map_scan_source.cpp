#include "planner/operator/scan/logical_scan_source.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::storage;
using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanSource(LogicalOperator* logicalOperator) {
    auto outSchema = logicalOperator->getSchema();
    auto& scanSource = logicalOperator->constCast<LogicalScanSource>();
    auto scanInfo = scanSource.getInfo();
    std::vector<DataPos> outPosV;
    outPosV.reserve(scanInfo->columns.size());
    for (auto& expr : scanInfo->columns) {
        outPosV.emplace_back(getDataPos(*expr, *outSchema));
    }
    auto info = TableFunctionCallInfo();
    info.function = scanInfo->func;
    info.bindData = scanInfo->bindData->copy();
    info.outPosV = outPosV;
    if (scanSource.hasOffset()) {
        info.rowOffsetPos = getDataPos(*scanSource.getOffset(), *outSchema);
    } else {
        info.rowOffsetPos = DataPos::getInvalidPos();
    }
    info.outputType =
        outPosV.empty() ? TableScanOutputType::EMPTY : TableScanOutputType::SINGLE_DATA_CHUNK;
    auto sharedState = std::make_shared<TableFunctionCallSharedState>();
    auto printInfo = std::make_unique<OPPrintInfo>(scanSource.getExpressionsForPrinting());
    return std::make_unique<TableFunctionCall>(std::move(info), sharedState, getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
