#pragma once

#include "catalog_entry.h"
#include "function_catalog_entry.h"

namespace kuzu {
namespace catalog {

class ScalarFunctionCatalogEntry final : public FunctionCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    ScalarFunctionCatalogEntry() = default;
    ScalarFunctionCatalogEntry(std::string name, function::function_set functionSet);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<CatalogEntry> copy() const override;
};

} // namespace catalog
} // namespace kuzu
