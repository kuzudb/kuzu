#pragma once

#include "catalog/property.h"
#include "catalog_entry.h"

namespace kuzu {
namespace catalog {

class TypeCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    TypeCatalogEntry() = default;
    TypeCatalogEntry(std::string name, common::LogicalType type)
        : CatalogEntry{CatalogEntryType::TYPE_ENTRY, std::move(name)}, type{std::move(type)} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::LogicalType getLogicalType() const { return type; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<TypeCatalogEntry> deserialize(common::Deserializer& deserializer);

private:
    common::LogicalType type;
};

} // namespace catalog
} // namespace kuzu
