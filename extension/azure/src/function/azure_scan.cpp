#include "function/azure_scan.h"

namespace kuzu {
namespace azure_extension {

using namespace function;
using namespace common;

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto scanInput = input->extraInput->constPtrCast<ExtraScanTableFuncBindInput>();
    auto connector = std::make_shared<AzureConnector>();
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, "" /* defaultSchemaName */,
        context);
    std::string query = stringFormat("SELECT * FROM '{}'", input->getLiteralVal<std::string>(0));
    auto result = connector->executeQuery(query + " LIMIT 1");
    std::vector<LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames = scanInput->expectedColumnNames;
    for (auto type : result->types) {
        returnTypes.push_back(
            duckdb_extension::DuckDBTypeConverter::convertDuckDBType(type.ToString()));
    }

    if (scanInput->expectedColumnNames.empty()) {
        for (auto name : result->names) {
            returnColumnNames.push_back(name);
        }
    }
    KU_ASSERT(returnTypes.size() == returnColumnNames.size());
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<AzureScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, columns, context);
}

std::unique_ptr<TableFuncSharedState> initAzureScanSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto azureScanBindData = input.bindData->constPtrCast<AzureScanBindData>();
    auto queryResult = azureScanBindData->connector->executeQuery(azureScanBindData->query);
    return std::make_unique<duckdb_extension::DuckDBScanSharedState>(std::move(queryResult));
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<duckdb_extension::DuckDBScanSharedState>();
    auto azureScanBindData = input.bindData->constPtrCast<AzureScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard lock{sharedState->mtx};
        result = sharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    azureScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set AzureScanFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initAzureScanSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace azure_extension
} // namespace kuzu
