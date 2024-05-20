#include "function/algorithm/graph_functions.h"
#include "function/algorithm/parallel_utils.h"
#include "planner/operator/logical_in_query_call.h"
#include "processor/operator/algorithm/algorithm_runner.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapInQueryCall(LogicalOperator* logicalOperator) {
    auto call = ku_dynamic_cast<LogicalOperator*, LogicalInQueryCall*>(logicalOperator);
    // TODO: The frontend is not steady for the table function call of graph algorithms.
    // This is temporary only for the purpose of this demo algorithm.
    if (call->getTableFunc().name == graph::DemoAlgorithm::name) {
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
        auto ftableSchema = std::make_unique<FactorizedTableSchema>();
        for (auto& expression : outSchema->getExpressionsInScope()) {
            auto dataPos = DataPos(outSchema->getExpressionPos(*expression));
            std::unique_ptr<ColumnSchema> columnSchema;
            if (outSchema->getGroup(dataPos.dataChunkPos)->isFlat()) {
                columnSchema = std::make_unique<ColumnSchema>(false /* isUnFlat */,
                    dataPos.dataChunkPos, LogicalTypeUtils::getRowLayoutSize(expression->dataType));
            } else {
                columnSchema = std::make_unique<ColumnSchema>(
                    true /* isUnFlat */, dataPos.dataChunkPos, (uint32_t)sizeof(overflow_value_t));
            }
            ftableSchema->appendColumn(std::move(columnSchema));
        }
        auto fTable = std::make_shared<FactorizedTable>(
            clientContext->getMemoryManager(), ftableSchema->copy());
        auto parallelUtils = std::make_shared<graph::ParallelUtils>(fTable);
        auto graphAlgorithm = std::make_shared<graph::DemoAlgorithm>();
        auto sharedState = std::make_shared<InQueryCallSharedState>();
        auto resultSetDescriptor = std::make_unique<ResultSetDescriptor>(outSchema);
        auto algorithmRunner = std::make_unique<AlgorithmRunner>(std::move(resultSetDescriptor),
            std::move(info), sharedState, std::move(ftableSchema), graphAlgorithm, parallelUtils,
            getOperatorID(), call->getExpressionsForPrinting());
        auto ftableScan = createFTableScanAligned(outSchema->getExpressionsInScope(), outSchema,
            call->getRowIDExpression(), fTable, 1 /* max morsel size for unflat column*/,
            std::move(algorithmRunner));
        return ftableScan;
    } else {
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
        return std::make_unique<InQueryCall>(
            std::move(info), sharedState, getOperatorID(), call->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
