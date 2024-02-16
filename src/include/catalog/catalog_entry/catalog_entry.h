#pragma once
#include <string>

#include "catalog_entry_type.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {
namespace catalog {

class CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructor & destructor
    //===--------------------------------------------------------------------===//
    CatalogEntry() = default;
    CatalogEntry(CatalogEntryType type, std::string name) : type{type}, name{std::move(name)} {}
    virtual ~CatalogEntry() = default;

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    CatalogEntryType getType() const { return type; }
    std::string getName() const { return name; }
    void rename(std::string newName) { name = std::move(newName); }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<CatalogEntry> deserialize(common::Deserializer& deserializer);
    virtual std::unique_ptr<CatalogEntry> copy() const = 0;

private:
    CatalogEntryType type;
    std::string name;
};

} // namespace catalog
} // namespace kuzu
