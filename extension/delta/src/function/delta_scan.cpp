#include "function/delta_scan.h"

namespace kuzu {
namespace delta_extension {

using namespace function;
using namespace common;

// Constructor implementation
DeltaScanBindData::DeltaScanBindData(std::string query, std::shared_ptr<DeltaConnector> connector,
    duckdb_extension::DuckDBResultConverter converter, std::vector<LogicalType> returnTypes,
    std::vector<std::string> returnColumnNames)
    : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames),
          1 /* maxOffset */},
      query{std::move(query)}, connector{std::move(connector)}, converter{std::move(converter)} {}

// Method implementation for copying
std::unique_ptr<TableFuncBindData> DeltaScanBindData::copy() const {
    return std::make_unique<DeltaScanBindData>(*this);
}

// This bindFunc now is sharing between delta_scan and iceberg_scan, maybe we can have a common
// template for them later.
std::unique_ptr<TableFuncBindData> bindFuncInternal(main::ClientContext* context,
    ScanTableFuncBindInput* input, const std::string& scanFuncName) {
    input->inputs[0].validateType(LogicalTypeID::STRING);

    // Construct connector
    auto connector = std::make_shared<DeltaConnector>();
    connector->format = scanFuncName;
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, context);

    std::string query;
    if (scanFuncName == "ICEBERG") {
        query = common::stringFormat("SELECT * FROM {}('{}', allow_moved_paths = true)",
            "ICEBERG_SCAN", input->inputs[0].getValue<std::string>());
    } else if (scanFuncName == "DELTA") {
        query = common::stringFormat("SELECT * FROM {}('{}')", "DELTA_SCAN",
            input->inputs[0].getValue<std::string>());
    }

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
    return std::make_unique<DeltaScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, copyVector(returnTypes),
        std::move(returnColumnNames));
}

// This bindFunc wraps the internal function to adhere to the TableFunction signature.
std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* input) {
    // Default to "DELTA".
    return bindFuncInternal(context, input, "DELTA");
}

struct DeltaScanSharedState : public function::TableFuncSharedState {
    explicit DeltaScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
        : queryResult{std::move(queryResult)} {}

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
    std::mutex lock;
};

std::unique_ptr<TableFuncSharedState> initDeltaScanSharedState(TableFunctionInitInput& input) {
    auto deltaScanBindData = input.bindData->constPtrCast<DeltaScanBindData>();
    auto queryResult = deltaScanBindData->connector->executeQuery(deltaScanBindData->query);
    return std::make_unique<DeltaScanSharedState>(std::move(queryResult));
}

common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState = input.sharedState->ptrCast<DeltaScanSharedState>();
    auto deltaScanBindData = input.bindData->constPtrCast<DeltaScanBindData>();
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
    deltaScanBindData->converter.convertDuckDBResultToVector(*result, output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set DeltaScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initDeltaScanSharedState,
            initEmptyLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace delta_extension
} // namespace kuzu
