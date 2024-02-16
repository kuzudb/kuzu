#include "catalog/catalog_entry/rel_group_catalog_entry.h"

namespace kuzu {
namespace catalog {

RelGroupCatalogEntry::RelGroupCatalogEntry(
    std::string tableName, common::table_id_t tableID, std::vector<common::table_id_t> relTableIDs)
    : TableCatalogEntry{CatalogEntryType::REL_GROUP_ENTRY, std::move(tableName), tableID},
      relTableIDs{std::move(relTableIDs)} {}

RelGroupCatalogEntry::RelGroupCatalogEntry(const RelGroupCatalogEntry& other)
    : TableCatalogEntry{other} {
    relTableIDs = other.relTableIDs;
}

bool RelGroupCatalogEntry::isParent(common::table_id_t childID) {
    auto it = find_if(relTableIDs.begin(), relTableIDs.end(),
        [&](common::table_id_t relTableID) { return relTableID == childID; });
    return it != relTableIDs.end();
}

void RelGroupCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.serializeVector(relTableIDs);
}

std::unique_ptr<RelGroupCatalogEntry> RelGroupCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::vector<common::table_id_t> relTableIDs;
    deserializer.deserializeVector(relTableIDs);
    auto relGroupEntry = std::make_unique<RelGroupCatalogEntry>();
    relGroupEntry->relTableIDs = std::move(relTableIDs);
    return relGroupEntry;
}

std::unique_ptr<CatalogEntry> RelGroupCatalogEntry::copy() const {
    return std::make_unique<RelGroupCatalogEntry>(*this);
}

} // namespace catalog
} // namespace kuzu
