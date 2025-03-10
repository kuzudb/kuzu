#include "function/sql_query.h"

#include "binder/binder.h"
#include "catalog/duckdb_catalog.h"
#include "common/exception/binder.h"
#include "connector/duckdb_type_converter.h"
#include "main/database_manager.h"
#include "storage/attached_postgres_database.h"
#include "storage/postgres_storage.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace postgres_extension {

struct SqlQuerySharedState final : function::TableFuncSharedState {
    explicit SqlQuerySharedState(std::shared_ptr<duckdb::MaterializedQueryResult> queryResult)
        : TableFuncSharedState{queryResult->RowCount()}, queryResult{std::move(queryResult)} {}

    std::shared_ptr<duckdb::MaterializedQueryResult> queryResult;
};

struct SqlQueryBindData final : function::TableFuncBindData {
    std::shared_ptr<duckdb::MaterializedQueryResult> queryResult;
    duckdb_extension::DuckDBResultConverter converter;

    SqlQueryBindData(std::shared_ptr<duckdb::MaterializedQueryResult> queryResult,
        duckdb_extension::DuckDBResultConverter converter, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1 /* maxOffset */},
          queryResult{std::move(queryResult)}, converter{std::move(converter)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<SqlQueryBindData>(*this);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    auto dbName = input->getLiteralVal<std::string>(0);
    auto query = input->getLiteralVal<std::string>(1);
    auto attachedDB = context->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB->getDBType() != PostgresStorageExtension::DB_TYPE) {
        throw common::BinderException{"sql queries can only be executed in attached postgres."};
    }
    auto queryToExecuteInDuckDB = common::stringFormat("select * from postgres_query({}, '{}');",
        attachedDB->constCast<AttachedPostgresDatabase>().getAttachedCatalogNameInDuckDB(), query);
    auto& attachedPostgresDB = attachedDB->constCast<AttachedPostgresDatabase>();
    auto queryResult = attachedPostgresDB.executeQuery(queryToExecuteInDuckDB);
    std::vector<common::LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    for (auto i = 0u; i < queryResult->names.size(); i++) {
        returnColumnNames.push_back(queryResult->ColumnName(i));
        returnTypes.push_back(duckdb_extension::DuckDBTypeConverter::convertDuckDBType(
            queryResult->types[i].ToString()));
    }
    duckdb_extension::DuckDBResultConverter duckdbResultConverter{returnTypes};
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<SqlQueryBindData>(std::move(queryResult),
        std::move(duckdbResultConverter), columns);
}

std::unique_ptr<TableFuncSharedState> initSharedState(const TableFunctionInitInput& input) {
    auto scanBindData = input.bindData->constPtrCast<SqlQueryBindData>();
    return std::make_unique<SqlQuerySharedState>(scanBindData->queryResult);
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<SqlQuerySharedState>();
    auto bindData = input.bindData->constPtrCast<SqlQueryBindData>();
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
