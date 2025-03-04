#pragma once

#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct CastToJsonFunction {
    static constexpr const char* name = "cast_to_json";

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
