#include "catalog/catalog_entry/external_table_catalog_entry.h"

#include "catalog/catalog_entry/external_node_table_catalog_entry.h"
#include "common/serializer/deserializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void ExternalTableCatalogEntry::copyFrom(const CatalogEntry& other) {
    TableCatalogEntry::copyFrom(other);
    auto& otherTable = other.constCast<ExternalTableCatalogEntry>();
    externalDBName = otherTable.externalDBName;
    externalTableName = otherTable.externalTableName;
    physicalEntry = otherTable.physicalEntry->copy();
}

void ExternalTableCatalogEntry::serialize(Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(externalDBName);
    serializer.write(externalTableName);
    KU_ASSERT(physicalEntry != nullptr);
    physicalEntry->serialize(serializer);
}

std::unique_ptr<ExternalTableCatalogEntry> ExternalTableCatalogEntry::deserialize(
    Deserializer& deserializer, CatalogEntryType type) {
    std::string externalDBName;
    std::string externalTableName;
    deserializer.deserializeValue(externalDBName);
    deserializer.deserializeValue(externalTableName);
    auto physicalEntry = CatalogEntry::deserialize(deserializer);
    std::unique_ptr<ExternalTableCatalogEntry> entry;
    switch (type) {
    case CatalogEntryType::EXTERNAL_NODE_TABLE_ENTRY: {
        entry = ExternalNodeTableCatalogEntry::deserialize(deserializer);
    } break;
    default:
        KU_UNREACHABLE;
    }
    entry->externalDBName = externalDBName;
    entry->externalTableName = externalTableName;
    entry->physicalEntry = physicalEntry->ptrCast<TableCatalogEntry>()->copy();
    return entry;
}

} // namespace catalog
} // namespace kuzu
