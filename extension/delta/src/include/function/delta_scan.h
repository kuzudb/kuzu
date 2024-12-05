#pragma once

#include "function/table/call_functions.h"

#include "connector/connector_factory.h"
#include "connector/delta_connector.h"
#include "connector/duckdb_result_converter.h"
#include "connector/duckdb_type_converter.h"

namespace kuzu {
namespace delta_extension {

using namespace function;
using namespace common;

struct DeltaScanFunction : function::CallFunction {
    static constexpr const char* name = "DELTA_SCAN";

    static function::function_set getFunctionSet();
};

struct DeltaScanBindData : public function::CallTableFuncBindData {
    std::string query;
    std::shared_ptr<DeltaConnector> connector;
    duckdb_extension::DuckDBResultConverter converter;

    DeltaScanBindData(std::string query, std::shared_ptr<DeltaConnector> connector,
        duckdb_extension::DuckDBResultConverter converter, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames);

    std::unique_ptr<TableFuncBindData> copy() const override;
};

// Functions and structs exposed for use
std::unique_ptr<function::TableFuncBindData> bindFuncInternal(
    main::ClientContext* context, function::ScanTableFuncBindInput* input, const std::string& scanFuncName);

std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context, ScanTableFuncBindInput* input);

std::unique_ptr<TableFuncSharedState> initDeltaScanSharedState(TableFunctionInitInput& input);

common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output);

} // namespace delta_extension
} // namespace kuzu
