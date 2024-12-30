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
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ClearCacheBindData>(context->getDatabaseManager(), columns,
        1 /* maxOffset */);
}

ClearCacheFunction::ClearCacheFunction() : TableFunction{name, std::vector<LogicalTypeID>{}} {
    tableFunc = clearCacheTableFunc;
    bindFunc = clearCacheBindFunc;
    initSharedStateFunc = SimpleTableFunction::initSharedState;
    initLocalStateFunc = SimpleTableFunction::initEmptyLocalState;
}

} // namespace duckdb_extension
} // namespace kuzu
