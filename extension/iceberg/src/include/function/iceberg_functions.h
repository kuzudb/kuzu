#pragma once

#include "common/string_utils.h"
#include "function/delta_scan.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;

struct IcebergScanFunction {
    static constexpr const char* name = "ICEBERG_SCAN";
    static function_set getFunctionSet();
};

struct IcebergMetadataFunction {
    static constexpr const char* name = "ICEBERG_METADATA";
    static function_set getFunctionSet();
};

struct IcebergSnapshotsFunction {
    static constexpr const char* name = "ICEBERG_SNAPSHOTS";
    static function_set getFunctionSet();
};

std::unique_ptr<TableFuncBindData> bindFuncHelper(main::ClientContext* context,
    const TableFuncBindInput* input, const std::string& tableType);

} // namespace iceberg_extension
} // namespace kuzu
