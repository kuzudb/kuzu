#pragma once

#include "catalog_entry.h"

namespace kuzu {
namespace catalog {

class IndexCatalogEntry : public CatalogEntry {
public:
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::INDEX_ENTRY;

public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    IndexCatalogEntry() = default;
    IndexCatalogEntry(std::string name, common::table_id_t tableID)
        : CatalogEntry{entryType_, std::move(name)}, tableID{tableID} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::table_id_t getTableID() const { return tableID; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& deserializer);

private:
    common::table_id_t tableID;
};

} // namespace catalog
} // namespace kuzu
