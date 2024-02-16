#pragma once

#include "catalog_entry.h"
#include "function_catalog_entry.h"

namespace kuzu {
namespace catalog {

class AggregateFunctionCatalogEntry : public FunctionCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    AggregateFunctionCatalogEntry() = default;
    AggregateFunctionCatalogEntry(std::string name, function::function_set functionSet);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<CatalogEntry> copy() const override;
};

} // namespace catalog
} // namespace kuzu
