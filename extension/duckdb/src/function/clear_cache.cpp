#include "function/clear_cache.h"

#include "catalog/duckdb_catalog.h"
#include "main/database_manager.h"
#include "storage/duckdb_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

static offset_t clearCacheTableFunc(const TableFuncInput& input,
    const TableFuncOutput& /*output*/) {
    const auto sharedState = input.sharedState->ptrCast<TableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<ClearCacheBindData>();
    bindData->databaseManager->invalidateCache();
    return 1;
}

static std::unique_ptr<TableFuncBindData> clearCacheBindFunc(const ClientContext* context,
    const TableFuncBindInput* /*input*/) {
    return std::make_unique<ClearCacheBindData>(context->getDatabaseManager());
}

function_set ClearCacheFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = clearCacheTableFunc;
    function->bindFunc = clearCacheBindFunc;
    function->initSharedStateFunc = TableFunction::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace duckdb_extension
} // namespace kuzu
