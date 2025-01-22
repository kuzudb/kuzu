#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct StemFunction {
    static constexpr const char* name = "STEM";
    static constexpr catalog::CatalogEntryType type =
        catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY;

    static function::function_set getFunctionSet();

    static void validateStemmer(const std::string& stemmer);
};

} // namespace fts_extension
} // namespace kuzu
