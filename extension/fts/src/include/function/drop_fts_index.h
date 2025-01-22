#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/table/simple_table_functions.h"

namespace kuzu {
namespace fts_extension {

struct DropFTSFunction : function::SimpleTableFunction {
    static constexpr const char* name = "DROP_FTS_INDEX";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
