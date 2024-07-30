#pragma once

#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct ToJsonFunction {
    static constexpr const char* name = "to_json";

    static function::function_set getFunctionSet();
};

// alias for castany to work with json
struct CastToJsonFunction : public ToJsonFunction {
    static constexpr const char* name = "cast_to_json";
};

struct JsonMergeFunction {
    static constexpr const char* name = "json_merge_patch";

    static function::function_set getFunctionSet();
};

struct JsonExtractFunction {
    static constexpr const char* name = "json_extract";

    static function::function_set getFunctionSet();
};

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
