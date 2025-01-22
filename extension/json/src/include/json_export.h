#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/function.h"

namespace kuzu {
namespace json_extension {

struct JsonExportFunction {
    static constexpr const char* name = "COPY_JSON";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

} // namespace json_extension
} // namespace kuzu
