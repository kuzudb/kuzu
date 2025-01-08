#include "catalog/fts_index_catalog_entry.h"

#include "catalog/catalog.h"
#include "common/serializer/buffered_reader.h"
#include "extension/extension_manager.h"
#include "fts_extension.h"
#include "main/client_context.h"

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
    std::vector<std::string> properties;
    auto config = FTSConfig::deserialize(deserializer);
    return std::make_unique<FTSIndexCatalogEntry>(indexCatalogEntry->getTableID(),
        indexCatalogEntry->getIndexName(), numDocs, avgDocLen, properties, config);
}

std::string FTSIndexCatalogEntry::toCypher(main::ClientContext* context) const {
    std::string cypher;
    auto catalog = context->getCatalog();
    auto tableName = catalog->getTableName(context->getTransaction(), tableID);
    std::string fieldsStr;
    for (auto i = 0u; i < properties.size(); i++) {
        fieldsStr +=
            common::stringFormat("'{}'{} ", properties[i], i == properties.size() - 1 ? "" : ",");
    }
    cypher += common::stringFormat("CALL CREATE_FTS_INDEX('{}', '{}', [{}], stemmer := '{}');\n",
        tableName, indexName, fieldsStr, config.stemmer);
    return cypher;
}

} // namespace fts_extension
} // namespace kuzu
