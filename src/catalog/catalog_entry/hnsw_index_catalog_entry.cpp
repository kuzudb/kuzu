#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"

#include "common/serializer/deserializer.h"

namespace kuzu {
namespace catalog {

void HNSWIndexCatalogEntry::serialize(common::Serializer& ser) const {
    CatalogEntry::serialize(ser);
    // TODO(Ziyi): tableID and indexName should be serialized inside IndexCatalogEntry.
    ser.writeDebuggingInfo("tableID");
    ser.serializeValue(tableID);
    ser.writeDebuggingInfo("indexName");
    ser.serializeValue(indexName);
    ser.writeDebuggingInfo("upperRelTableID");
    ser.serializeValue(upperRelTableID);
    ser.writeDebuggingInfo("lowerRelTableID");
    ser.serializeValue(lowerRelTableID);
    ser.writeDebuggingInfo("columnName");
    ser.serializeValue(columnName);
    ser.writeDebuggingInfo("upperEntryPoint");
    ser.serializeValue(upperEntryPoint);
    ser.writeDebuggingInfo("lowerEntryPoint");
    ser.serializeValue(lowerEntryPoint);
    ser.writeDebuggingInfo("config");
    config.serialize(ser);
}

std::unique_ptr<IndexCatalogEntry> HNSWIndexCatalogEntry::deserialize(common::Deserializer& deSer) {
    common::table_id_t tableID;
    std::string indexName;
    common::table_id_t upperRelTableID;
    common::table_id_t lowerRelTableID;
    std::string columnName;
    common::offset_t upperEntryPoint;
    common::offset_t lowerEntryPoint;
    std::string debuggingInfo;
    deSer.validateDebuggingInfo(debuggingInfo, "tableID");
    deSer.deserializeValue(tableID);
    deSer.validateDebuggingInfo(debuggingInfo, "indexName");
    deSer.deserializeValue(indexName);
    deSer.validateDebuggingInfo(debuggingInfo, "upperRelTableID");
    deSer.deserializeValue(upperRelTableID);
    deSer.validateDebuggingInfo(debuggingInfo, "lowerRelTableID");
    deSer.deserializeValue(lowerRelTableID);
    deSer.validateDebuggingInfo(debuggingInfo, "columnName");
    deSer.deserializeValue(columnName);
    deSer.validateDebuggingInfo(debuggingInfo, "upperEntryPoint");
    deSer.deserializeValue(upperEntryPoint);
    deSer.validateDebuggingInfo(debuggingInfo, "lowerEntryPoint");
    deSer.deserializeValue(lowerEntryPoint);
    deSer.validateDebuggingInfo(debuggingInfo, "config");
    auto config = storage::HNSWIndexConfig::deserialize(deSer);
    return std::make_unique<HNSWIndexCatalogEntry>(tableID, indexName, columnName, upperRelTableID,
        lowerRelTableID, upperEntryPoint, lowerEntryPoint, std::move(config));
}

std::unique_ptr<IndexCatalogEntry> HNSWIndexCatalogEntry::copy() const {
    return std::make_unique<HNSWIndexCatalogEntry>(tableID, indexName, columnName, upperRelTableID,
        lowerRelTableID, upperEntryPoint, lowerEntryPoint, config);
}

} // namespace catalog
} // namespace kuzu
