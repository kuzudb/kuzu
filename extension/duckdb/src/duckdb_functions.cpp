#include "duckdb_functions.h"

#include "duckdb_catalog.h"
#include "duckdb_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

static offset_t clearCacheTableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto bindData = input.bindData->constPtrCast<ClearCacheBindData>();
    bindData->databaseManager->invalidateCache();
    dataChunk.getValueVector(0)->setValue<std::string>(0,
        "All attached database caches have been cleared.");
    return 1;
}

static std::unique_ptr<TableFuncBindData> clearCacheBindFunc(ClientContext* context,
    TableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("message");
    columnTypes.emplace_back(LogicalType::STRING());
    return std::make_unique<ClearCacheBindData>(context->getDatabaseManager(),
        std::move(columnTypes), std::move(columnNames), 1 /* maxOffset */);
}

ClearCacheFunction::ClearCacheFunction()
    : TableFunction{name, clearCacheTableFunc, clearCacheBindFunc,
          function::CallFunction::initSharedState, function::CallFunction::initEmptyLocalState,
          std::vector<LogicalTypeID>{}} {}

} // namespace duckdb_extension
} // namespace kuzu
