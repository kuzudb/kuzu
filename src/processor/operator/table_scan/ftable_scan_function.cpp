#include "processor/operator/table_scan/ftable_scan_function.h"

#include "function/table/simple_table_function.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

struct FTableScanSharedState final : public SimpleTableFuncSharedState {
    std::shared_ptr<FactorizedTable> table;
    uint64_t morselSize;
    offset_t nextTupleIdx;

    FTableScanSharedState(std::shared_ptr<FactorizedTable> table, uint64_t morselSize)
        : SimpleTableFuncSharedState{table->getNumTuples()}, table{std::move(table)},
          morselSize{morselSize}, nextTupleIdx{0} {}

    TableFuncMorsel getMorsel() override {
        std::unique_lock lck{mtx};
        auto numTuplesToScan = std::min(morselSize, table->getNumTuples() - nextTupleIdx);
        auto morsel = TableFuncMorsel(nextTupleIdx, nextTupleIdx + numTuplesToScan);
        nextTupleIdx += numTuplesToScan;
        return morsel;
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = ku_dynamic_cast<FTableScanSharedState*>(input.sharedState);
    auto bindData = ku_dynamic_cast<FTableScanBindData*>(input.bindData);
    auto morsel = sharedState->getMorsel();
    if (morsel.endOffset <= morsel.startOffset) {
        return 0;
    }
    auto numTuples = morsel.endOffset - morsel.startOffset;
    sharedState->table->scan(output.vectors, morsel.startOffset, numTuples,
        bindData->columnIndices);
    return numTuples;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(const TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<FTableScanBindData*>(input.bindData);
    return std::make_unique<FTableScanSharedState>(bindData->table, bindData->morselSize);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(const TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

function_set FTableScan::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
