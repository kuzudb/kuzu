#include "function/delta_scan.h"

#include "connector/connector_factory.h"
#include "connector/delta_connector.h"
#include "connector/duckdb_result_converter.h"
#include "connector/duckdb_type_converter.h"
#include "function/table/scan_functions.h"
#include "binder/binder.h"

namespace kuzu {
namespace delta_extension {

using namespace function;
using namespace common;

struct DeltaScanBindData : public ScanBindData {
    std::string query;
    std::shared_ptr<DeltaConnector> connector;
    duckdb_extension::DuckDBResultConverter converter;

    DeltaScanBindData(std::string query, std::shared_ptr<DeltaConnector> connector,
        duckdb_extension::DuckDBResultConverter converter, binder::expression_vector columns, ReaderConfig config, main::ClientContext* ctx)
        : ScanBindData{std::move(columns), std::move(config), ctx},
          query{std::move(query)}, connector{std::move(connector)},
          converter{std::move(converter)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DeltaScanBindData>(*this);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    auto connector = std::make_shared<DeltaConnector>();
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, context);
    std::string query = common::stringFormat("SELECT * FROM DELTA_SCAN('{}')",
        input->getLiteralVal<std::string>(0));
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
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<DeltaScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, columns, ReaderConfig{}, context);
}

struct DeltaScanSharedState : public BaseScanSharedState {
    explicit DeltaScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult)
        : BaseScanSharedState{}, queryResult{std::move(queryResult)} {}

    uint64_t getNumRows() const override { return queryResult->RowCount(); }

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
};

std::unique_ptr<TableFuncSharedState> initDeltaScanSharedState(TableFunctionInitInput& input) {
    auto deltaScanBindData = input.bindData->constPtrCast<DeltaScanBindData>();
    auto queryResult = deltaScanBindData->connector->executeQuery(deltaScanBindData->query);
    return std::make_unique<DeltaScanSharedState>(std::move(queryResult));
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
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

std::unique_ptr<TableFuncLocalState> initEmptyLocalState(TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
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
