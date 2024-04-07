#include "catalog/catalog_entry/function_catalog_entry.h"

#include "common/utils.h"

namespace kuzu {
namespace catalog {

FunctionCatalogEntry::FunctionCatalogEntry(CatalogEntryType entryType, std::string name,
    function::function_set functionSet)
    : CatalogEntry{entryType, std::move(name)}, functionSet{std::move(functionSet)} {}

std::unique_ptr<CatalogEntry> FunctionCatalogEntry::copy() const {
    return std::make_unique<FunctionCatalogEntry>(getType(), getName(),
        common::copyVector<std::unique_ptr<function::Function>>(functionSet));
}

} // namespace catalog
} // namespace kuzu
