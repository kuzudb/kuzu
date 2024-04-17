#pragma once

#include "catalog_entry.h"
#include "function/scalar_macro_function.h"

namespace kuzu {
namespace catalog {

class ScalarMacroCatalogEntry final : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    ScalarMacroCatalogEntry() = default;
    ScalarMacroCatalogEntry(std::string name,
        std::unique_ptr<function::ScalarMacroFunction> macroFunction);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    function::ScalarMacroFunction* getMacroFunction() const { return macroFunction.get(); }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<ScalarMacroCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* clientContext) const override;

private:
    std::unique_ptr<function::ScalarMacroFunction> macroFunction;
};

} // namespace catalog
} // namespace kuzu
