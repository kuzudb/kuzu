#include "catalog/catalog_entry/index_catalog_entry.h"

#include <catalog/catalog_entry/hnsw_index_catalog_entry.h>

namespace kuzu {
namespace catalog {

void IndexCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(type);
    serializer.write(tableID);
    serializer.write(indexName);
    serializeAuxInfo(serializer);
}

std::unique_ptr<IndexCatalogEntry> IndexCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string type;
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(tableID);
    deserializer.deserializeValue(indexName);
    auto indexEntry = std::make_unique<IndexCatalogEntry>(type, tableID, std::move(indexName));
    uint64_t auxBufferSize = 0;
    deserializer.deserializeValue(auxBufferSize);
    indexEntry->auxBuffer = std::make_unique<uint8_t[]>(auxBufferSize);
    indexEntry->auxBufferSize = auxBufferSize;
    deserializer.read(indexEntry->auxBuffer.get(), auxBufferSize);
    if (type == HNSWIndexCatalogEntry::TYPE_NAME) {
        return HNSWIndexCatalogEntry::deserializeAuxInfo(indexEntry.get());
    }
    return indexEntry;
}

void IndexCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherTable = other.constCast<IndexCatalogEntry>();
    tableID = otherTable.tableID;
    indexName = otherTable.indexName;
}

} // namespace catalog
} // namespace kuzu
