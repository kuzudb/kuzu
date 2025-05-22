#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "storage/page_manager.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace function {

struct FreeSpaceInfoBindData final : TableFuncBindData {
    const main::ClientContext* ctx;
    FreeSpaceInfoBindData(binder::expression_vector columns, common::row_idx_t numRows,
        const main::ClientContext* ctx)
        : TableFuncBindData{std::move(columns), numRows}, ctx{ctx} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FreeSpaceInfoBindData>(columns, numRows, ctx);
    }
};

static common::offset_t internalTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, common::DataChunk& output) {
    const auto bindData = input.bindData->constPtrCast<FreeSpaceInfoBindData>();
    const auto entries =
        bindData->ctx->getStorageManager()->getDataFH()->getPageManager()->getFreeEntries(
            morsel.startOffset, morsel.endOffset);
    for (common::row_idx_t i = 0; i < entries.size(); ++i) {
        const auto& freeEntry = entries[i];
        output.getValueVectorMutable(0).setValue<uint64_t>(i, freeEntry.startPageIdx);
        output.getValueVectorMutable(1).setValue<uint64_t>(i, freeEntry.numPages);
    }
    return entries.size();
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames = {"start_page_idx", "num_pages"};
    std::vector<common::LogicalType> columnTypes;
    columnTypes.push_back(common::LogicalType::UINT64());
    columnTypes.push_back(common::LogicalType::UINT64());
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<FreeSpaceInfoBindData>(columns,
        context->getStorageManager()->getDataFH()->getPageManager()->getNumFreeEntries(), context);
}

function_set FreeSpaceInfoFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<common::LogicalTypeID>{});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
