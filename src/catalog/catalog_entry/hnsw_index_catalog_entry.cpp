#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/serializer/buffered_reader.h"
#include "common/serializer/buffered_serializer.h"
#include "common/serializer/deserializer.h"
#include "main/client_context.h"

namespace kuzu {
namespace catalog {

std::shared_ptr<common::BufferedSerializer> HNSWIndexAuxInfo::serialize() const {
    auto bufferWriter = std::make_shared<common::BufferedSerializer>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.serializeValue(upperRelTableID);
    serializer.serializeValue(lowerRelTableID);
    serializer.serializeValue(upperEntryPoint);
    serializer.serializeValue(lowerEntryPoint);
    config.serialize(serializer);
    return bufferWriter;
}

std::unique_ptr<HNSWIndexAuxInfo> HNSWIndexAuxInfo::deserialize(
    std::unique_ptr<common::BufferReader> reader) {
    common::table_id_t upperRelTableID = common::INVALID_TABLE_ID;
    common::table_id_t lowerRelTableID = common::INVALID_TABLE_ID;
    common::offset_t upperEntryPoint = common::INVALID_OFFSET;
    common::offset_t lowerEntryPoint = common::INVALID_OFFSET;
    common::Deserializer deSer{std::move(reader)};
    deSer.deserializeValue(upperRelTableID);
    deSer.deserializeValue(lowerRelTableID);
    deSer.deserializeValue(upperEntryPoint);
    deSer.deserializeValue(lowerEntryPoint);
    auto config = storage::HNSWIndexConfig::deserialize(deSer);
    return std::make_unique<HNSWIndexAuxInfo>(upperRelTableID, lowerRelTableID, upperEntryPoint,
        lowerEntryPoint, std::move(config));
}

std::string HNSWIndexAuxInfo::toCypher(const IndexCatalogEntry& indexEntry,
    const ToCypherInfo& info) const {
    auto& indexToCypherInfo = info.constCast<IndexToCypherInfo>();
    auto context = indexToCypherInfo.context;
    std::string cypher;
    auto catalog = context->getCatalog();
    auto tableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), indexEntry.getTableID());
    auto tableName = tableEntry->getName();
    auto propertyName = tableEntry->getProperty(indexEntry.getPropertyIDs()[0]).getName();
    auto distFuncName = storage::HNSWIndexConfig::distFuncToString(config.distFunc);
    cypher += common::stringFormat("CALL CREATE_HNSW_INDEX('{}', '{}', '{}', mu := {}, ml := {}, "
                                   "pu := {}, distFunc := '{}', alpha := {}, efc := {});",
        tableName, indexEntry.getIndexName(), propertyName, config.mu, config.ml, config.pu,
        distFuncName, config.alpha, config.efc);
    return cypher;
}

} // namespace catalog
} // namespace kuzu
