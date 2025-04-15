#include "function/sql_query.h"

#include "binder/binder.h"
#include "catalog/duckdb_catalog.h"
#include "common/exception/binder.h"
#include "connector/duckdb_type_converter.h"
#include "function/duckdb_scan.h"
#include "main/database_manager.h"
#include "processor/execution_context.h"
#include "storage/attached_postgres_database.h"
#include "storage/postgres_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::duckdb_extension;

namespace kuzu {
namespace postgres_extension {

// SQL based database uses single quote `'` as the escape character. We have to manually escape
// all `'`.
static std::string escapeSpecialChars(const std::string& pgQuery) {
    std::string pgQueryWithEscapeChars = "";
    for (auto& i : pgQuery) {
        if (i == '\'') {
            pgQueryWithEscapeChars += "'";
        }
        pgQueryWithEscapeChars += i;
    }
    return pgQueryWithEscapeChars;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    auto dbName = input->getLiteralVal<std::string>(0);
    auto query = input->getLiteralVal<std::string>(1);
    auto attachedDB = context->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB->getDBType() != PostgresStorageExtension::DB_TYPE) {
        throw common::BinderException{"sql queries can only be executed in attached postgres."};
    }
    auto queryTemplate =
        "select {} " +
        common::stringFormat("from postgres_query({}, '{}')",
            attachedDB->constCast<AttachedPostgresDatabase>().getAttachedCatalogNameInDuckDB(),
            escapeSpecialChars(query));
    // Query to sniff the column names and types.
    auto queryToExecuteInDuckDB = common::stringFormat(queryTemplate, "*") + " limit 1";
    auto& attachedPostgresDB = attachedDB->constCast<AttachedPostgresDatabase>();
    auto queryResult = attachedPostgresDB.executeQuery(queryToExecuteInDuckDB);
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> columnNamesInPG;
    for (auto i = 0u; i < queryResult->names.size(); i++) {
        columnNamesInPG.push_back(queryResult->ColumnName(i));
        returnTypes.push_back(
            DuckDBTypeConverter::convertDuckDBType(queryResult->types[i].ToString()));
    }
    auto returnColumnNames =
        TableFunction::extractYieldVariables(columnNamesInPG, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<DuckDBScanBindData>(queryTemplate, std::move(columnNamesInPG),
        attachedPostgresDB.getConnector(), columns);
}

std::unique_ptr<TableFuncSharedState> initSharedState(const TableFuncInitSharedStateInput& input) {
    auto scanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    auto finalQuery = stringFormat(scanBindData->query, scanBindData->getColumnsToSelect());
    auto result = scanBindData->connector.executeQuery(finalQuery);
    return std::make_unique<DuckDBScanSharedState>(std::move(result));
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<DuckDBScanSharedState>();
    auto bindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard<std::mutex> lock{sharedState->mtx};
        result = sharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    bindData->converter.convertDuckDBResultToVector(*result, output.dataChunk,
        bindData->getColumnSkips());
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set SqlQueryFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace postgres_extension
} // namespace kuzu
