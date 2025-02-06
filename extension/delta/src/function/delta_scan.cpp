#include "function/delta_scan.h"

namespace kuzu {
namespace delta_extension {

using namespace function;
using namespace common;

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    auto connector = std::make_shared<DeltaConnector>();
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, "" /* defaultSchemaName */,
        context);
    std::string query =
        stringFormat("SELECT * FROM DELTA_SCAN('{}')", input->getLiteralVal<std::string>(0));
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
    return std::make_unique<DeltaScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, columns, FileScanInfo{}, context);
}

struct DeltaScanSharedState final : BaseScanSharedState {
    explicit DeltaScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
        : BaseScanSharedState{}, queryResult{std::move(queryResult)} {}

    uint64_t getNumRows() const override { return queryResult->RowCount(); }

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
};

std::unique_ptr<TableFuncSharedState> initDeltaScanSharedState(
    const TableFunctionInitInput& input) {
    auto deltaScanBindData = input.bindData->constPtrCast<DeltaScanBindData>();
    auto queryResult = deltaScanBindData->connector->executeQuery(deltaScanBindData->query);
    return std::make_unique<DeltaScanSharedState>(std::move(queryResult));
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<DeltaScanSharedState>();
    auto deltaScanBindData = input.bindData->constPtrCast<DeltaScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard lock{sharedState->lock};
        result = sharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    deltaScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

std::unique_ptr<TableFuncLocalState> initEmptyLocalState(const TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

function_set DeltaScanFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initDeltaScanSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace delta_extension
} // namespace kuzu
