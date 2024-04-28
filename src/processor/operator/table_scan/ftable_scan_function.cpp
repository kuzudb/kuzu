#include "processor/operator/table_scan/ftable_scan_function.h"

#include "function/table/scan_functions.h"
#include "function/table_functions.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

struct FTableScanMorsel {
    uint64_t startTupleIdx;
    uint64_t numTuples;

    FTableScanMorsel(uint64_t startTupleIdx, uint64_t numTuples)
        : startTupleIdx{startTupleIdx}, numTuples{numTuples} {}
};

struct FTableScanSharedState final : public function::BaseScanSharedState {
    std::shared_ptr<FactorizedTable> table;
    uint64_t morselSize;
    common::offset_t nextTupleIdx;

    FTableScanSharedState(std::shared_ptr<FactorizedTable> table, uint64_t morselSize)
        : BaseScanSharedState{}, table{std::move(table)}, morselSize{morselSize}, nextTupleIdx{0} {}

    FTableScanMorsel getMorsel() {
        std::unique_lock lck{lock};
        auto numTuplesToScan = std::min(morselSize, table->getNumTuples() - nextTupleIdx);
        auto morsel = FTableScanMorsel(nextTupleIdx, numTuplesToScan);
        nextTupleIdx += numTuplesToScan;
        return morsel;
    }

    uint64_t getNumRows() const override {
        KU_ASSERT(table->getNumTuples() == table->getTotalNumFlatTuples());
        return table->getNumTuples();
    }
};

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, FTableScanSharedState*>(input.sharedState);
    auto bindData = ku_dynamic_cast<TableFuncBindData*, FTableScanBindData*>(input.bindData);
    auto morsel = sharedState->getMorsel();
    if (morsel.numTuples == 0) {
        return 0;
    }
    sharedState->table->scan(output.vectors, morsel.startTupleIdx, morsel.numTuples,
        bindData->columnIndices);
    return morsel.numTuples;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, FTableScanBindData*>(input.bindData);
    return std::make_unique<FTableScanSharedState>(bindData->table, bindData->morselSize);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

function_set FTableScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, nullptr /*bindFunc*/,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
