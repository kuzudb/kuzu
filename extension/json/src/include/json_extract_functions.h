#pragma once

#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct JsonExtractFunction {
    static constexpr const char* name = "json_extract";

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
