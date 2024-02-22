#include "catalog/catalog_entry/table_function_catalog_entry.h"

#include "common/utils.h"

namespace kuzu {
namespace catalog {

TableFunctionCatalogEntry::TableFunctionCatalogEntry(
    std::string name, function::function_set functionSet)
    : FunctionCatalogEntry{
          CatalogEntryType::TABLE_FUNCTION_ENTRY, std::move(name), std::move(functionSet)} {}

std::unique_ptr<CatalogEntry> TableFunctionCatalogEntry::copy() const {
    return std::make_unique<TableFunctionCatalogEntry>(getName(), common::copyVector(functionSet));
}

} // namespace catalog
} // namespace kuzu
