#pragma once

#include "function/table/table_function.h"

namespace kuzu {
namespace json_extension {

struct JsonScan {
    static constexpr const char* name = "JSON_SCAN";

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
