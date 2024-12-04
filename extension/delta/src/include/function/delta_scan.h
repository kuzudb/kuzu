#pragma once

#include "function/table/call_functions.h"

namespace kuzu {
namespace delta_extension {

struct DeltaScanFunction : function::CallFunction {
    static constexpr const char* name = "DELTA_SCAN";

    static function::function_set getFunctionSet();
};

} // namespace delta_extension
} // namespace kuzu
