#include "catalog/fts_index_catalog_entry.h"

#include "common/serializer/buffered_reader.h"

namespace kuzu {
namespace fts_extension {

std::unique_ptr<catalog::IndexCatalogEntry> FTSIndexCatalogEntry::copy() const {
    auto other = std::make_unique<FTSIndexCatalogEntry>();
    other->numDocs = numDocs;
    other->avgDocLen = avgDocLen;
    other->copyFrom(*this);
    return other;
}

void FTSIndexCatalogEntry::serializeAuxInfo(common::Serializer& serializer) const {
    auto len = sizeof(numDocs) + sizeof(avgDocLen) + config.getNumBytesForSerialization();
    serializer.serializeValue(len);
    serializer.serializeValue(numDocs);
    serializer.serializeValue(avgDocLen);
    config.serialize(serializer);
}

std::unique_ptr<FTSIndexCatalogEntry> FTSIndexCatalogEntry::deserializeAuxInfo(
    catalog::IndexCatalogEntry* indexCatalogEntry) {
    auto reader = std::make_unique<common::BufferReader>(indexCatalogEntry->getAuxBuffer(),
        indexCatalogEntry->getAuxBufferSize());
    common::Deserializer deserializer{std::move(reader)};
    common::idx_t numDocs = 0;
    deserializer.deserializeValue(numDocs);
    double avgDocLen = 0;
    deserializer.deserializeValue(avgDocLen);
    auto config = FTSConfig::deserialize(deserializer);
    return std::make_unique<FTSIndexCatalogEntry>(indexCatalogEntry->getTableID(),
        indexCatalogEntry->getIndexName(), numDocs, avgDocLen, config);
}

} // namespace fts_extension
} // namespace kuzu
