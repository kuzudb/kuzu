#include "catalog/catalog_entry/scalar_function_catalog_entry.h"

#include "common/utils.h"

namespace kuzu {
namespace catalog {

ScalarFunctionCatalogEntry::ScalarFunctionCatalogEntry(
    std::string name, function::function_set functionSet)
    : FunctionCatalogEntry{
          CatalogEntryType::SCALAR_FUNCTION_ENTRY, std::move(name), std::move(functionSet)} {}

std::unique_ptr<CatalogEntry> ScalarFunctionCatalogEntry::copy() const {
    return std::make_unique<ScalarFunctionCatalogEntry>(getName(), common::copyVector(functionSet));
}

} // namespace catalog
} // namespace kuzu
