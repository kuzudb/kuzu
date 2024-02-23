#include "catalog/catalog_entry/aggregate_function_catalog_entry.h"

#include "common/utils.h"

namespace kuzu {
namespace catalog {

AggregateFunctionCatalogEntry::AggregateFunctionCatalogEntry(
    std::string name, function::function_set functionSet)
    : FunctionCatalogEntry{
          CatalogEntryType::AGGREGATE_FUNCTION_ENTRY, std::move(name), std::move(functionSet)} {}

std::unique_ptr<CatalogEntry> AggregateFunctionCatalogEntry::copy() const {
    return std::make_unique<AggregateFunctionCatalogEntry>(
        getName(), common::copyVector(functionSet));
}

} // namespace catalog
} // namespace kuzu
