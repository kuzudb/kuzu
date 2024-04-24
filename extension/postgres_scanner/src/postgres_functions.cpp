#include "postgres_functions.h"

#include "duckdb_functions.h"
#include "postgres_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace postgres_scanner {

struct PostgresClearCacheBindData : public duckdb_scanner::DuckDBClearCacheBindData {

    PostgresClearCacheBindData(DatabaseManager* databaseManager,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : DuckDBClearCacheBindData{databaseManager, std::move(returnTypes),
              std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PostgresClearCacheBindData>(databaseManager, columnTypes,
            columnNames, maxOffset);
    }
};

static offset_t DuckDBClearCacheTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto bindData =
        ku_dynamic_cast<function::TableFuncBindData*, PostgresClearCacheBindData*>(input.bindData);
    bindData->databaseManager->invalidateCache(PostgresStorageExtension::dbType);
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
    return std::make_unique<PostgresClearCacheBindData>(context->getDatabaseManager(),
        std::move(columnTypes), std::move(columnNames), 1 /* maxOffset */);
}

PostgresClearCacheFunction::PostgresClearCacheFunction()
    : TableFunction{name, DuckDBClearCacheTableFunc, DuckDBClearCacheBindFunc,
          function::CallFunction::initSharedState, function::CallFunction::initEmptyLocalState,
          std::vector<LogicalTypeID>{}} {}

} // namespace postgres_scanner
} // namespace kuzu
