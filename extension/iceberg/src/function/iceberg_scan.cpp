#include "function/iceberg_scan.h"

#include "connector/connector_factory.h"
#include "connector/iceberg_connector.h"
#include "connector/duckdb_result_converter.h"
#include "duckdb_type_converter.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;
using namespace common;

struct IcebergScanBindData : public CallTableFuncBindData {
    std::string query;
    std::shared_ptr<IcebergConnector> connector;
    duckdb_extension::DuckDBResultConverter converter;

    IcebergScanBindData(std::string query, std::shared_ptr<IcebergConnector> connector,
        duckdb_extension::DuckDBResultConverter converter, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames),
              1 /* maxOffset */},
          query{std::move(query)}, connector{std::move(connector)},
          converter{std::move(converter)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<IcebergScanBindData>(*this);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* input) {
    auto connector = std::make_shared<IcebergConnector>();
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, context);
    input->inputs[0].validateType(LogicalTypeID::STRING);
    std::string query = common::stringFormat("SELECT * FROM ICEBERG_SCAN('{}', allow_moved_paths = true)",
        input->inputs[0].getValue<std::string>());
    auto result = connector->executeQuery(query + " LIMIT 1");
    std::vector<LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    for (auto type : result->types) {
        returnTypes.push_back(
            duckdb_extension::DuckDBTypeConverter::convertDuckDBType(type.ToString()));
    }

    for (auto name : result->names) {
        returnColumnNames.push_back(name);
    }
    KU_ASSERT(returnTypes.size() == returnColumnNames.size());
    return std::make_unique<IcebergScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, copyVector(returnTypes),
        std::move(returnColumnNames));
}

struct IcebergScanSharedState : public function::TableFuncSharedState {
    explicit IcebergScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
        : queryResult{std::move(queryResult)} {}

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
    std::mutex lock;
};

std::unique_ptr<TableFuncSharedState> initIcebergScanSharedState(TableFunctionInitInput& input) {
    auto icebergScanBindData = input.bindData->constPtrCast<IcebergScanBindData>();
    auto queryResult = icebergScanBindData->connector->executeQuery(icebergScanBindData->query);
    return std::make_unique<IcebergScanSharedState>(std::move(queryResult));
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<IcebergScanSharedState>();
    auto icebergScanBindData = input.bindData->constPtrCast<IcebergScanBindData>();
    std::unique_ptr<duckdb::DataChunk> result;
    try {
        // Duckdb queryResult.fetch() is not thread safe, we have to acquire a lock there.
        std::lock_guard<std::mutex> lock{sharedState->lock};
        result = sharedState->queryResult->Fetch();
    } catch (std::exception& e) {
        return 0;
    }
    if (result == nullptr) {
        return 0;
    }
    icebergScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set IcebergScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initIcebergScanSharedState,
            initEmptyLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace iceberg_extension
} // namespace kuzu
