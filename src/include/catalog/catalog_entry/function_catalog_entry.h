#pragma once

#include "catalog_entry.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace catalog {

class FunctionCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    FunctionCatalogEntry() = default;
    FunctionCatalogEntry(
        CatalogEntryType entryType, std::string name, function::function_set functionSet);

    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    function::function_set& getFunctionSet() { return functionSet; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    // We always register functions while initializing the catalog, so we don't have to
    // serialize functions.
    void serialize(common::Serializer& /*serializer*/) const override { return; }
    std::unique_ptr<CatalogEntry> copy() const override;

protected:
    function::function_set functionSet;
};

} // namespace catalog
} // namespace kuzu
