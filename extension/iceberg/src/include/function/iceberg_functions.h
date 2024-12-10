#pragma once

#include "common/string_utils.h"
#include "connector/connector_factory.h"
#include "connector/duckdb_result_converter.h"
#include "connector/duckdb_type_converter.h"
#include "function/delta_scan.h"
#include "function/table/simple_table_functions.h"
#include "main/delta_extension.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;

struct IcebergScanFunction {
    static constexpr const char* name = "ICEBERG_SCAN";
    static function::function_set getFunctionSet();
};

struct IcebergMetadataFunction {
    static constexpr const char* name = "ICEBERG_METADATA";
    static function::function_set getFunctionSet();
};

struct IcebergSnapshotsFunction {
    static constexpr const char* name = "ICEBERG_SNAPSHOTS";
    static function::function_set getFunctionSet();
};

std::unique_ptr<TableFuncBindData> bindFuncHelper(main::ClientContext* context,
    TableFuncBindInput* input, const std::string& tableType);

} // namespace iceberg_extension
} // namespace kuzu
