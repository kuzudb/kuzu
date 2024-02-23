#pragma once

#include "function_catalog_entry.h"

namespace kuzu {
namespace catalog {

class TableFunctionCatalogEntry : public FunctionCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    TableFunctionCatalogEntry() = default;
    TableFunctionCatalogEntry(std::string name, function::function_set functionSet);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<CatalogEntry> copy() const override;
};

} // namespace catalog
} // namespace kuzu
