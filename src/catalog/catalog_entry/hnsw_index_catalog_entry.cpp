#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"

#include "common/serializer/deserializer.h"
#include <common/serializer/buffered_reader.h>
#include <common/serializer/buffered_serializer.h>

namespace kuzu {
namespace catalog {

void HNSWIndexCatalogEntry::serializeAuxInfo(common::Serializer& ser) const {
    ser.serializeValue(getNumBytesForSerialization());
    ser.serializeValue(upperRelTableID);
    ser.serializeValue(lowerRelTableID);
    ser.serializeValue(columnName);
    ser.serializeValue(upperEntryPoint);
    ser.serializeValue(lowerEntryPoint);
    config.serialize(ser);
}

std::unique_ptr<HNSWIndexCatalogEntry> HNSWIndexCatalogEntry::deserializeAuxInfo(
    const IndexCatalogEntry* indexCatalogEntry) {
    auto reader = std::make_unique<common::BufferReader>(indexCatalogEntry->getAuxBuffer(),
        indexCatalogEntry->getAuxBufferSize());

    common::table_id_t upperRelTableID = common::INVALID_TABLE_ID;
    common::table_id_t lowerRelTableID = common::INVALID_TABLE_ID;
    std::string columnName;
    common::offset_t upperEntryPoint = common::INVALID_OFFSET;
    common::offset_t lowerEntryPoint = common::INVALID_OFFSET;
    std::string debuggingInfo;
    common::Deserializer deSer{std::move(reader)};
    deSer.deserializeValue(upperRelTableID);
    deSer.deserializeValue(lowerRelTableID);
    deSer.deserializeValue(columnName);
    deSer.deserializeValue(upperEntryPoint);
    deSer.deserializeValue(lowerEntryPoint);
    auto config = storage::HNSWIndexConfig::deserialize(deSer);
    return std::make_unique<HNSWIndexCatalogEntry>(indexCatalogEntry->getTableID(),
        indexCatalogEntry->getIndexName(), columnName, upperRelTableID, lowerRelTableID,
        upperEntryPoint, lowerEntryPoint, std::move(config));
}

std::unique_ptr<IndexCatalogEntry> HNSWIndexCatalogEntry::copy() const {
    return std::make_unique<HNSWIndexCatalogEntry>(tableID, indexName, columnName, upperRelTableID,
        lowerRelTableID, upperEntryPoint, lowerEntryPoint, config.copy());
}

uint64_t HNSWIndexCatalogEntry::getNumBytesForSerialization() const {
    auto bufferWriter = std::make_shared<common::BufferedSerializer>();
    auto serializer = common::Serializer(bufferWriter);
    serializer.serializeValue(upperEntryPoint);
    serializer.serializeValue(lowerEntryPoint);
    serializer.serializeValue(columnName);
    serializer.serializeValue(upperEntryPoint);
    serializer.serializeValue(lowerEntryPoint);
    config.serialize(serializer);
    return bufferWriter->getSize();
}

} // namespace catalog
} // namespace kuzu
