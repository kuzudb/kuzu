#include "duckdb_functions.h"

#include "duckdb_catalog.h"
#include "duckdb_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

static offset_t DuckDBClearCacheTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto bindData = input.bindData->constPtrCast<DuckDBClearCacheBindData>();
    bindData->databaseManager->invalidateCache(DuckDBStorageExtension::dbType);
    dataChunk.getValueVector(0)->setValue<std::string>(0,
        "All attached duckdb database caches have been cleared.");
    return 1;
}

static std::unique_ptr<TableFuncBindData> DuckDBClearCacheBindFunc(ClientContext* context,
    TableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("message");
    columnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<DuckDBClearCacheBindData>(context->getDatabaseManager(),
        std::move(columnTypes), std::move(columnNames), 1 /* maxOffset */);
}

DuckDBClearCacheFunction::DuckDBClearCacheFunction()
    : TableFunction{name, DuckDBClearCacheTableFunc, DuckDBClearCacheBindFunc,
          function::CallFunction::initSharedState, function::CallFunction::initEmptyLocalState,
          std::vector<LogicalTypeID>{}} {}

} // namespace duckdb_extension
} // namespace kuzu
