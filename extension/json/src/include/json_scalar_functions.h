#pragma once

#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct JsonArrayLengthFunction {
    static constexpr const char* name = "json_array_length";

    static function::function_set getFunctionSet();
};

struct JsonContainsFunction {
    static constexpr const char* name = "json_contains";

    static function::function_set getFunctionSet();
};

struct JsonKeysFunction {
    static constexpr const char* name = "json_keys";

    static function::function_set getFunctionSet();
};

struct JsonStructureFunction {
    static constexpr const char* name = "json_structure";

    static function::function_set getFunctionSet();
};

struct JsonTypeFunction {
    static constexpr const char* name = "json_type";

    static function::function_set getFunctionSet();
};

struct JsonValidFunction {
    static constexpr const char* name = "json_valid";

    static function::function_set getFunctionSet();
};

struct MinifyJsonFunction {
    static constexpr const char* name = "json";

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
