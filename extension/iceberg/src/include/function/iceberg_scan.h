#pragma once

#include "function/table/call_functions.h"

namespace kuzu {
namespace iceberg_extension {

struct IcebergScanFunction : function::CallFunction {
    static constexpr const char* name = "ICEBERG_SCAN";

    static function::function_set getFunctionSet();
};

} // namespace iceberg_extension
} // namespace kuzu
