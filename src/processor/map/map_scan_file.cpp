#include "planner/operator/scan/logical_scan_file.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::storage;
using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanFile(LogicalOperator* logicalOperator) {
    auto outSchema = logicalOperator->getSchema();
    auto scanFile = ku_dynamic_cast<LogicalOperator*, LogicalScanFile*>(logicalOperator);
    auto scanFileInfo = scanFile->getInfo();
    std::vector<DataPos> outPosV;
    outPosV.reserve(scanFileInfo->columns.size());
    for (auto& expr : scanFileInfo->columns) {
        outPosV.emplace_back(getDataPos(*expr, *outSchema));
    }
    auto info = InQueryCallInfo();
    info.function = scanFileInfo->func;
    info.bindData = scanFileInfo->bindData->copy();
    info.outPosV = outPosV;
    if (scanFile->hasOffset()) {
        info.rowOffsetPos = getDataPos(*scanFile->getOffset(), *outSchema);
    } else {
        info.rowOffsetPos = DataPos::getInvalidPos();
    }
    info.outputType =
        outPosV.empty() ? TableScanOutputType::EMPTY : TableScanOutputType::SINGLE_DATA_CHUNK;
    auto sharedState = std::make_shared<InQueryCallSharedState>();
    return std::make_unique<InQueryCall>(
        std::move(info), sharedState, getOperatorID(), scanFile->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
