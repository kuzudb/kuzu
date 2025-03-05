#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct ToJsonFunction {
    static constexpr const char* name = "to_json";

    static function::function_set getFunctionSet();

    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        const std::vector<common::SelectionVector*>& parameterSelVectors,
        common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/);
};

struct JsonQuoteFunction {
    using alias = ToJsonFunction;

    static constexpr const char* name = "json_quote";
};

struct ArrayToJsonFunction {
    using alias = ToJsonFunction;

    static constexpr const char* name = "array_to_json";
};

struct RowToJsonFunction {
    using alias = ToJsonFunction;

    static constexpr const char* name = "row_to_json";
};

struct JsonArrayFunction {
    static constexpr const char* name = "json_array";

    static function::function_set getFunctionSet();
};

struct JsonObjectFunction {
    static constexpr const char* name = "json_object";

    static function::function_set getFunctionSet();
};

struct JsonMergePatchFunction {
    static constexpr const char* name = "json_merge_patch";

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
