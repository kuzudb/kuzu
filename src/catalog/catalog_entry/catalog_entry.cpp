#include "catalog/catalog_entry/catalog_entry.h"

#include "catalog/catalog_entry/table_catalog_entry.h"

namespace kuzu {
namespace catalog {

void CatalogEntry::serialize(common::Serializer& serializer) const {
    serializer.write(static_cast<uint8_t>(type));
    serializer.write(name);
}

std::unique_ptr<CatalogEntry> CatalogEntry::deserialize(common::Deserializer& deserializer) {
    CatalogEntryType type;
    std::string name;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(name);
    std::unique_ptr<CatalogEntry> entry;
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY:
    case CatalogEntryType::RDF_GRAPH_ENTRY:
        entry = TableCatalogEntry::deserialize(deserializer, type);
        break;
    default:
        KU_UNREACHABLE;
    }
    entry->type = type;
    entry->name = std::move(name);
    return entry;
}

} // namespace catalog
} // namespace kuzu
