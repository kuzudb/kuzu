#include "catalog/catalog_entry/index_catalog_entry.h"

#include "common/serializer/deserializer.h"

namespace kuzu {
namespace catalog {

void IndexCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.writeDebuggingInfo("index");
    serializer.serializeValue(tableID);
}

std::unique_ptr<IndexCatalogEntry> IndexCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string debuggingInfo;
    auto indexCatalogEntry = std::make_unique<IndexCatalogEntry>();
    deserializer.validateDebuggingInfo(debuggingInfo, "index");
    deserializer.deserializeValue(indexCatalogEntry->tableID);
    return indexCatalogEntry;
}

} // namespace catalog
} // namespace kuzu
