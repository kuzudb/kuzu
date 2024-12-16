#pragma once

#include "connector/connector_factory.h"
#include "connector/delta_connector.h"
#include "connector/duckdb_result_converter.h"
#include "connector/duckdb_type_converter.h"
#include "function/table/scan_functions.h"
#include "function/table/simple_table_functions.h"
#include "binder/binder.h"

namespace kuzu {
namespace delta_extension {

using namespace function;
using namespace common;

struct DeltaScanFunction {
    static constexpr const char* name = "DELTA_SCAN";

    static function::function_set getFunctionSet();
};

struct DeltaScanBindData : public ScanBindData {
    std::string query;
    std::shared_ptr<DeltaConnector> connector;
    duckdb_extension::DuckDBResultConverter converter;

    DeltaScanBindData(std::string query, std::shared_ptr<DeltaConnector> connector,
        duckdb_extension::DuckDBResultConverter converter, binder::expression_vector columns,
        ReaderConfig config, main::ClientContext* ctx)
        : ScanBindData{std::move(columns), std::move(config), ctx}, query{std::move(query)},
          connector{std::move(connector)}, converter{std::move(converter)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DeltaScanBindData>(*this);
    }
};

// Functions and structs exposed for use
std::unique_ptr<TableFuncSharedState> initDeltaScanSharedState(TableFunctionInitInput& input);

std::unique_ptr<TableFuncLocalState> initEmptyLocalState(TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*);

common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output);

} // namespace delta_extension
} // namespace kuzu
