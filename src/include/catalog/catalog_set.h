#pragma once

#include "catalog_entry/catalog_entry.h"
#include "common/case_insensitive_map.h"

namespace kuzu {
namespace catalog {

class CatalogSet {
public:
    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    bool containsEntry(const std::string& name) const;
    CatalogEntry* getEntry(const std::string& name);
    KUZU_API void createEntry(std::unique_ptr<CatalogEntry> entry);
    void removeEntry(const std::string& name);
    void renameEntry(const std::string& oldName, const std::string& newName);
    common::case_insensitive_map_t<std::unique_ptr<CatalogEntry>>& getEntries() { return entries; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer serializer) const;
    static std::unique_ptr<CatalogSet> deserialize(common::Deserializer& deserializer);
    std::unique_ptr<CatalogSet> copy() const;

private:
    common::case_insensitive_map_t<std::unique_ptr<CatalogEntry>> entries;
};

} // namespace catalog
} // namespace kuzu
