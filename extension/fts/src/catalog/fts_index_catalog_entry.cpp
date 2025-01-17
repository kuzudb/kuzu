#include "catalog/fts_index_catalog_entry.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/serializer/buffered_reader.h"
#include "common/serializer/buffered_serializer.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

std::shared_ptr<common::BufferedSerializer> FTSIndexAuxInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferedSerializer>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.serializeValue(numDocs);
    serializer.serializeValue(avgDocLen);
    serializer.serializeVector(properties);
    config.serialize(serializer);
    return bufferWriter;
}

std::unique_ptr<FTSIndexAuxInfo> FTSIndexAuxInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::Deserializer deserializer{std::move(reader)};
    common::idx_t numDocs = 0;
    deserializer.deserializeValue(numDocs);
    double avgDocLen = 0;
    deserializer.deserializeValue(avgDocLen);
    std::vector<std::string> properties;
    deserializer.deserializeVector(properties);
    auto config = FTSConfig::deserialize(deserializer);
    return std::make_unique<FTSIndexAuxInfo>(numDocs, avgDocLen, std::move(properties),
        std::move(config));
}

std::string FTSIndexAuxInfo::toCypher(const catalog::IndexCatalogEntry& indexEntry,
    main::ClientContext* context) {
    std::string cypher;
    auto catalog = context->getCatalog();
    auto tableName = catalog->getTableCatalogEntry(context->getTransaction(), indexEntry.getTableID())->getName();
    std::string propertyStr;
    for (auto i = 0u; i < properties.size(); i++) {
        propertyStr +=
            common::stringFormat("'{}'{} ", properties[i], i == properties.size() - 1 ? "" : ",");
    }
    cypher += common::stringFormat("CALL CREATE_FTS_INDEX('{}', '{}', [{}], stemmer := '{}');\n",
        tableName, indexEntry.getIndexName(), std::move(propertyStr), config.stemmer);
    return cypher;
}

} // namespace fts_extension
} // namespace kuzu
