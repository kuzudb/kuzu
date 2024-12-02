#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "storage/index/hnsw_config.h"

namespace kuzu {
namespace catalog {

class HNSWIndexCatalogEntry final : public IndexCatalogEntry {
public:
    HNSWIndexCatalogEntry(common::table_id_t tableID, std::string indexName, std::string columnName,
        common::table_id_t upperRelTableID, common::table_id_t lowerRelTableID,
        common::offset_t upperEntryPoint, common::offset_t lowerEntryPoint,
        storage::HNSWIndexConfig config)
        : IndexCatalogEntry{tableID, std::move(indexName)}, upperRelTableID{upperRelTableID},
          lowerRelTableID{lowerRelTableID}, columnName{std::move(columnName)},
          upperEntryPoint{upperEntryPoint}, lowerEntryPoint{lowerEntryPoint},
          config{std::move(config)} {}

    void serialize(common::Serializer& ser) const override;
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& deSer);

    std::unique_ptr<IndexCatalogEntry> copy() const override;

    common::table_id_t getUpperRelTableID() const { return upperRelTableID; }
    common::table_id_t getLowerRelTableID() const { return lowerRelTableID; }
    const std::string& getIndexColumnName() const { return columnName; }
    common::offset_t getUpperEntryPoint() const { return upperEntryPoint; }
    common::offset_t getLowerEntryPoint() const { return lowerEntryPoint; }
    const storage::HNSWIndexConfig& getConfig() const { return config; }
    storage::HNSWIndexConfig getConfig() { return config; }

private:
    common::table_id_t upperRelTableID;
    common::table_id_t lowerRelTableID;
    std::string columnName;
    common::offset_t upperEntryPoint;
    common::offset_t lowerEntryPoint;
    storage::HNSWIndexConfig config;
};

} // namespace catalog
} // namespace kuzu
