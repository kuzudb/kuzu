#include <utility>

#include "catalog/catalog.h"
#include "function/built_in_function_utils.h"
#include "processor/operator/table_function_call.h"
#include "processor/operator/table_scan/ftable_scan_function.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScan(const expression_vector& exprs,
    std::vector<ft_col_idx_t> colIndices, std::shared_ptr<Expression> offset, Schema* schema,
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize, physical_op_vector_t children) {
    std::vector<DataPos> outPosV;
    outPosV.reserve(exprs.size());
    for (auto i = 0u; i < exprs.size(); ++i) {
        outPosV.emplace_back(schema->getExpressionPos(*exprs[i]));
    }
    auto bindData =
        std::make_unique<FTableScanBindData>(table, std::move(colIndices), maxMorselSize);
    auto function = function::BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
        FTableScan::name, clientContext->getCatalog()->getFunctions(clientContext->getTx()));
    auto info = TableFunctionCallInfo();
    info.function = *ku_dynamic_cast<TableFunction*>(function);
    info.bindData = std::move(bindData);
    info.outPosV = std::move(outPosV);
    if (offset != nullptr) {
        info.rowOffsetPos = getDataPos(*offset, *schema);
    } else {
        info.rowOffsetPos = DataPos::getInvalidPos(); // Invalid data pos.
    }
    info.outputType = TableScanOutputType::MULTI_DATA_CHUNK;
    auto sharedState = std::make_shared<TableFunctionCallSharedState>();
    auto printInfo = std::make_unique<FTableScanFunctionCallPrintInfo>(function->name, exprs);
    if (children.size() == 0) {
        return std::make_unique<TableFunctionCall>(std::move(info), sharedState, getOperatorID(),
            std::move(printInfo));
    }

    return std::make_unique<TableFunctionCall>(std::move(info), sharedState, std::move(children),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScan(const expression_vector& exprs,
    std::vector<ft_col_idx_t> colIndices, Schema* schema, std::shared_ptr<FactorizedTable> table,
    uint64_t maxMorselSize, physical_op_vector_t children) {
    return createFTableScan(exprs, colIndices, nullptr /* offset */, schema, std::move(table),
        maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScan(const expression_vector& exprs,
    std::vector<ft_col_idx_t> colIndices, Schema* schema, std::shared_ptr<FactorizedTable> table,
    uint64_t maxMorselSize) {
    physical_op_vector_t children;
    return createFTableScan(exprs, colIndices, nullptr /* offset */, schema, std::move(table),
        maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createEmptyFTableScan(
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize, physical_op_vector_t children) {
    return createFTableScan(expression_vector{}, std::vector<ft_col_idx_t>{}, nullptr /* offset */,
        std::move(table), maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createEmptyFTableScan(
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
    std::unique_ptr<PhysicalOperator> child) {
    physical_op_vector_t children;
    children.push_back(std::move(child));
    return createFTableScan(expression_vector{}, std::vector<ft_col_idx_t>{}, nullptr /* offset */,
        std::move(table), maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createEmptyFTableScan(
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize) {
    physical_op_vector_t children;
    return createFTableScan(expression_vector{}, std::vector<ft_col_idx_t>{}, nullptr /* offset */,
        std::move(table), maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScanAligned(
    const expression_vector& exprs, Schema* schema, std::shared_ptr<Expression> offset,
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize, physical_op_vector_t children) {
    std::vector<ft_col_idx_t> colIndices;
    colIndices.reserve(exprs.size());
    for (auto i = 0u; i < exprs.size(); ++i) {
        colIndices.push_back(i);
    }
    return createFTableScan(exprs, std::move(colIndices), std::move(offset), schema, table,
        maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScanAligned(
    const expression_vector& exprs, Schema* schema, std::shared_ptr<FactorizedTable> table,
    uint64_t maxMorselSize, physical_op_vector_t children) {
    return createFTableScanAligned(exprs, schema, nullptr /* offset*/, std::move(table),
        maxMorselSize, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFTableScanAligned(
    const expression_vector& exprs, planner::Schema* schema, std::shared_ptr<FactorizedTable> table,
    uint64_t maxMorselSize) {
    physical_op_vector_t children;
    return createFTableScanAligned(exprs, schema, std::move(table), maxMorselSize,
        std::move(children));
}

} // namespace processor
} // namespace kuzu
