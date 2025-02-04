#include "function/duckdb_scan.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "connector/duckdb_connector.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace duckdb_extension {

DuckDBScanBindData::DuckDBScanBindData(std::string query,
    const std::vector<LogicalType>& columnTypes, std::vector<std::string> columnNames,
    const DuckDBConnector& connector)
    : query{std::move(query)}, columnTypes{LogicalType::copy(columnTypes)},
      columnNames{std::move(columnNames)}, converter{columnTypes}, connector{connector} {}

DuckDBScanSharedState::DuckDBScanSharedState(
    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
    : BaseScanSharedStateWithNumRows{queryResult->RowCount()}, queryResult{std::move(queryResult)} {
}

struct DuckDBScanFunction {
    static constexpr char DUCKDB_SCAN_FUNC_NAME[] = "duckdb_scan";

    static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output);

    static std::unique_ptr<TableFuncBindData> bindFunc(std::shared_ptr<DuckDBScanBindData> bindData,
        main::ClientContext* /*context*/, const TableFuncBindInput* input);

    static std::unique_ptr<TableFuncSharedState> initSharedState(
        const TableFunctionInitInput& input);

    static std::unique_ptr<TableFuncLocalState> initLocalState(const TableFunctionInitInput& input,
        TableFuncSharedState* state, storage::MemoryManager*);
};

std::unique_ptr<TableFuncSharedState> DuckDBScanFunction::initSharedState(
    const TableFunctionInitInput& input) {
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
    auto finalQuery =
        stringFormat(scanBindData->getQuery(input.context), columnNames) + predicatesString;
    auto result = scanBindData->connector.executeQuery(finalQuery);
    if (result->HasError()) {
        throw RuntimeException(
            stringFormat("Failed to execute query due to error: {}", result->GetError()));
    }
    return std::make_unique<DuckDBScanSharedState>(std::move(result));
}

std::unique_ptr<TableFuncLocalState> DuckDBScanFunction::initLocalState(
    const TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

offset_t DuckDBScanFunction::tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
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

std::unique_ptr<TableFuncBindData> DuckDBScanFunction::bindFunc(
    std::shared_ptr<DuckDBScanBindData> bindData, main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto result = bindData->copy();
    auto scanBindData = bindData->constPtrCast<DuckDBScanBindData>();
    auto columnNames =
        TableFunction::extractYieldVariables(scanBindData->columnNames, input->yieldVariables);
    result->columns =
        input->binder->createVariables(columnNames, scanBindData->getColumnTypes(*context));
    return result;
}

TableFunction getScanFunction(std::shared_ptr<DuckDBScanBindData> bindData) {
    auto function =
        TableFunction(DuckDBScanFunction::DUCKDB_SCAN_FUNC_NAME, std::vector<LogicalTypeID>{});
    function.tableFunc = DuckDBScanFunction::tableFunc;
    function.bindFunc = std::bind(DuckDBScanFunction::bindFunc, bindData, std::placeholders::_1,
        std::placeholders::_2);
    function.initSharedStateFunc = DuckDBScanFunction::initSharedState;
    function.initLocalStateFunc = DuckDBScanFunction::initLocalState;
    return function;
}

} // namespace duckdb_extension
} // namespace kuzu
