#include "planner/operator/scan/logical_scan_file.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::storage;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanFile(LogicalOperator* logicalOperator) {
    auto outSchema = logicalOperator->getSchema();
    auto scanFile = reinterpret_cast<LogicalScanFile*>(logicalOperator);
    auto info = scanFile->getInfo();
    std::vector<DataPos> dataColumnsPos;
    dataColumnsPos.reserve(info->columns.size());
    for (auto& expression : info->columns) {
        dataColumnsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto inQueryCallFuncInfo =
        std::make_unique<InQueryCallInfo>(info->copyFunc, info->bindData->copy(),
            std::move(dataColumnsPos), DataPos(outSchema->getExpressionPos(*info->offset)));
    return std::make_unique<InQueryCall>(std::move(inQueryCallFuncInfo),
        std::make_shared<InQueryCallSharedState>(), PhysicalOperatorType::IN_QUERY_CALL,
        getOperatorID(), scanFile->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
