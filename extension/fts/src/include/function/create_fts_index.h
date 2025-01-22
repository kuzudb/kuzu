#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/table/simple_table_functions.h"

namespace kuzu {
namespace fts_extension {

struct CreateFTSFunction : function::SimpleTableFunction {
    static constexpr const char* name = "CREATE_FTS_INDEX";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY;
    static constexpr const char* DOC_LEN_PROP_NAME = "LEN";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
