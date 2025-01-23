#include "function/clear_cache.h"

#include "binder/binder.h"
#include "catalog/duckdb_catalog.h"
#include "main/database_manager.h"
#include "storage/duckdb_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

static offset_t clearCacheTableFunc(const TableFuncInput& input, const TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<ClearCacheBindData>();
    bindData->databaseManager->invalidateCache();
    dataChunk.getValueVectorMutable(0).setValue<std::string>(0,
        "All attached database caches have been cleared.");
    return 1;
}

static std::unique_ptr<TableFuncBindData> clearCacheBindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("message");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames = SimpleTableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ClearCacheBindData>(context->getDatabaseManager(), columns,
        1 /* maxOffset */);
}

function_set ClearCacheFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = clearCacheTableFunc;
    function->bindFunc = clearCacheBindFunc;
    function->initSharedStateFunc = SimpleTableFunction::initSharedState;
    function->initLocalStateFunc = SimpleTableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace duckdb_extension
} // namespace kuzu
