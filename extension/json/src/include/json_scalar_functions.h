#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct JsonArrayLengthFunction {
    static constexpr const char* name = "json_array_length";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonContainsFunction {
    static constexpr const char* name = "json_contains";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonKeysFunction {
    static constexpr const char* name = "json_keys";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonStructureFunction {
    static constexpr const char* name = "json_structure";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonTypeFunction {
    static constexpr const char* name = "json_type";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct JsonValidFunction {
    static constexpr const char* name = "json_valid";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

struct MinifyJsonFunction {
    static constexpr const char* name = "json";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
