#include "catalog/catalog_entry/function_catalog_entry.h"

namespace kuzu {
namespace catalog {

FunctionCatalogEntry::FunctionCatalogEntry(CatalogEntryType entryType, std::string name,
    function::function_set functionSet)
    : CatalogEntry{entryType, std::move(name)}, functionSet{std::move(functionSet)} {}

} // namespace catalog
} // namespace kuzu
