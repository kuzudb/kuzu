#pragma once
#include <string>

#include "catalog_entry_type.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"

namespace kuzu {
namespace catalog {

class KUZU_API CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructor & destructor
    //===--------------------------------------------------------------------===//
    CatalogEntry() = default;
    CatalogEntry(CatalogEntryType type, std::string name) : type{type}, name{std::move(name)} {}
    CatalogEntry(const CatalogEntry& other)
        : type{other.type}, name{std::move(other.name)}, comment{std::move(other.comment)} {}
    virtual ~CatalogEntry() = default;

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    CatalogEntryType getType() const { return type; }
    std::string getName() const { return name; }
    std::string getComment() const { return comment; }
    void rename(std::string newName) { name = std::move(newName); }
    void setComment(std::string newComment) { comment = std::move(newComment); }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<CatalogEntry> deserialize(common::Deserializer& deserializer);
    virtual std::unique_ptr<CatalogEntry> copy() const = 0;
    virtual std::string toCypher(main::ClientContext* /*clientContext*/) const { KU_UNREACHABLE; }

private:
    CatalogEntryType type;
    std::string name;
    std::string comment;
};

} // namespace catalog
} // namespace kuzu
