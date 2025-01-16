#pragma once

#include "catalog_entry.h"

namespace kuzu {
namespace catalog {

class RelGroupCatalogEntry final : public CatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string name, std::vector<common::table_id_t> relTableIDs)
        : CatalogEntry{type_, name}, relTableIDs{std::move(relTableIDs)} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    const std::vector<common::table_id_t>& getRelTableIDs() const { return relTableIDs; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* clientContext) const override;

private:
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
