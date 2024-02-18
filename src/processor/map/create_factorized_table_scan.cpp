#include <utility>

#include "binder/expression/expression_util.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/table_scan/ftable_scan_function.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::createFactorizedTableScan(
    const expression_vector& expressions, std::vector<ft_col_idx_t> colIndices, Schema* schema,
    const std::shared_ptr<FactorizedTable>& table, uint64_t maxMorselSize,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> outPosV;
    outPosV.reserve(expressions.size());
    for (auto i = 0u; i < expressions.size(); ++i) {
        outPosV.emplace_back(schema->getExpressionPos(*expressions[i]));
    }
    auto bindData =
        std::make_unique<FTableScanBindData>(table, std::move(colIndices), maxMorselSize);
    auto function = function::BuiltInFunctionsUtils::matchFunction(
        READ_FTABLE_FUNC_NAME, catalog->getFunctions(clientContext->getTx()));
    auto info = InQueryCallInfo();
    info.function = ku_dynamic_cast<Function*, TableFunction*>(function);
    info.bindData = std::move(bindData);
    info.outPosV = std::move(outPosV);
    info.rowOffsetPos = DataPos(); // Invalid data pos.
    info.outputType = TableScanOutputType::MULTI_DATA_CHUNK;
    auto sharedState = std::make_shared<InQueryCallSharedState>();
    if (prevOperator == nullptr) {
        return std::make_unique<InQueryCall>(
            std::move(info), sharedState, getOperatorID(), ExpressionUtil::toString(expressions));
    }
    return std::make_unique<InQueryCall>(std::move(info), sharedState, std::move(prevOperator),
        getOperatorID(), ExpressionUtil::toString(expressions));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFactorizedTableScanAligned(
    const expression_vector& expressions, Schema* schema,
    const std::shared_ptr<FactorizedTable>& table, uint64_t maxMorselSize,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<ft_col_idx_t> columnIndices;
    columnIndices.reserve(expressions.size());
    for (auto i = 0u; i < expressions.size(); ++i) {
        columnIndices.push_back(i);
    }
    return createFactorizedTableScan(expressions, std::move(columnIndices), schema, table,
        maxMorselSize, std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
