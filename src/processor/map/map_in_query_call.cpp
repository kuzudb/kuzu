#include "planner/operator/logical_in_query_call.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapInQueryCall(LogicalOperator* logicalOperator) {
    auto call = ku_dynamic_cast<LogicalOperator*, LogicalInQueryCall*>(logicalOperator);
    std::vector<DataPos> outPosV;
    auto outSchema = call->getSchema();
    for (auto& expr : call->getOutputExpressions()) {
        outPosV.emplace_back(getDataPos(*expr, *outSchema));
    }
    auto info = InQueryCallInfo();
    info.function = call->getTableFunc();
    info.bindData = call->getBindData()->copy();
    info.outPosV = outPosV;
    info.rowOffsetPos = getDataPos(*call->getRowIDExpression(), *outSchema);
    info.outputType =
        outPosV.empty() ? TableScanOutputType::EMPTY : TableScanOutputType::SINGLE_DATA_CHUNK;
    auto sharedState = std::make_shared<InQueryCallSharedState>();
    return std::make_unique<InQueryCall>(std::move(info), sharedState, getOperatorID(),
        call->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
