#include "function/duckdb_scan.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "connector/duckdb_connector.h"
#include "function/table/bind_input.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

DuckDBScanBindData::DuckDBScanBindData(std::string query,
    std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames,
    const DuckDBConnector& connector)
    : TableFuncBindData{}, query{std::move(query)}, columnTypes{LogicalType::copy(columnTypes)},
      columnNames{columnNames}, converter{columnTypes}, connector{connector} {}

DuckDBScanSharedState::DuckDBScanSharedState(
    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
    : BaseScanSharedStateWithNumRows{queryResult->RowCount()}, queryResult{std::move(queryResult)} {
}

struct DuckDBScanFunction {
    static constexpr char DUCKDB_SCAN_FUNC_NAME[] = "duckdb_scan";

    static common::offset_t tableFunc(function::TableFuncInput& input,
        function::TableFuncOutput& output);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(DuckDBScanBindData bindData,
        main::ClientContext* /*context*/, function::TableFuncBindInput* input);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* state,
        storage::MemoryManager*
        /*mm*/);
};

std::unique_ptr<function::TableFuncSharedState> DuckDBScanFunction::initSharedState(
    function::TableFunctionInitInput& input) {
    auto scanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::string columnNames = "";
    auto columnSkips = scanBindData->getColumnSkips();
    auto numSkippedColumns =
        std::count_if(columnSkips.begin(), columnSkips.end(), [](auto item) { return item; });
    if (scanBindData->getNumColumns() == numSkippedColumns) {
        columnNames = input.bindData->constPtrCast<DuckDBScanBindData>()->columnNames[0];
    }
    for (auto i = 0u; i < scanBindData->getNumColumns(); i++) {
        if (columnSkips[i]) {
            continue;
        }
        columnNames += input.bindData->constPtrCast<DuckDBScanBindData>()->columnNames[i];
        columnNames += (i == scanBindData->getNumColumns() - 1) ? "" : ",";
    }
    std::string predicatesString = "";
    for (auto& predicates : scanBindData->getColumnPredicates()) {
        if (predicates.isEmpty()) {
            continue;
        }
        if (predicatesString.empty()) {
            predicatesString = " WHERE " + predicates.toString();
        } else {
            predicatesString += stringFormat(" AND {}", predicates.toString());
        }
    }
    auto finalQuery = common::stringFormat(scanBindData->query, columnNames) + predicatesString;
    auto result = scanBindData->connector.executeQuery(finalQuery);
    if (result->HasError()) {
        throw common::RuntimeException(
            common::stringFormat("Failed to execute query due to error: {}", result->GetError()));
    }
    return std::make_unique<DuckDBScanSharedState>(std::move(result));
}

std::unique_ptr<function::TableFuncLocalState> DuckDBScanFunction::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

common::offset_t DuckDBScanFunction::tableFunc(function::TableFuncInput& input,
    function::TableFuncOutput& output) {
    auto duckdbScanSharedState = input.sharedState->ptrCast<DuckDBScanSharedState>();
    auto duckdbScanBindData = input.bindData->constPtrCast<DuckDBScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard<std::mutex> lock{duckdbScanSharedState->lock};
        result = duckdbScanSharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    duckdbScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk,
        duckdbScanBindData->getColumnSkips());
    return output.dataChunk.state->getSelVector().getSelSize();
}

std::unique_ptr<function::TableFuncBindData> DuckDBScanFunction::bindFunc(
    DuckDBScanBindData bindData, main::ClientContext* /*clientContext*/,
    function::TableFuncBindInput* input) {
    auto result = bindData.copy();
    result->columns = input->binder->createVariables(bindData.columnNames, bindData.columnTypes);
    return result;
}

TableFunction getScanFunction(DuckDBScanBindData bindData) {
    auto function =
        TableFunction(DuckDBScanFunction::DUCKDB_SCAN_FUNC_NAME, std::vector<LogicalTypeID>{});
    function.tableFunc = DuckDBScanFunction::tableFunc;
    function.bindFunc = std::bind(DuckDBScanFunction::bindFunc, std::move(bindData),
        std::placeholders::_1, std::placeholders::_2);
    function.initSharedStateFunc = DuckDBScanFunction::initSharedState;
    function.initLocalStateFunc = DuckDBScanFunction::initLocalState;
    return function;
}

} // namespace duckdb_extension
} // namespace kuzu
