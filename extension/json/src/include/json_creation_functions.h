#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct ToJsonFunction {
    static constexpr const char* name = "to_json";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
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

struct CastToJsonFunction {
    using alias = ToJsonFunction;

    static constexpr const char* name = "cast_to_json";
};

struct JsonArrayFunction {
    static constexpr const char* name = "json_array";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonObjectFunction {
    static constexpr const char* name = "json_object";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonMergePatchFunction {
    static constexpr const char* name = "json_merge_patch";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
