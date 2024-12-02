#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"

namespace kuzu {
namespace catalog {

class HNSWIndexCatalogEntry final : public IndexCatalogEntry {
public:
    HNSWIndexCatalogEntry(common::table_id_t tableID, std::string indexName, std::string columnName,
        common::table_id_t upperRelTableID, common::table_id_t lowerRelTableID,
        common::offset_t upperEntryPoint, common::offset_t lowerEntryPoint)
        : IndexCatalogEntry{tableID, std::move(indexName)}, upperRelTableID{upperRelTableID},
          lowerRelTableID{lowerRelTableID}, columnName{std::move(columnName)},
          upperEntryPoint{upperEntryPoint}, lowerEntryPoint{lowerEntryPoint} {}

    std::unique_ptr<IndexCatalogEntry> copy() const override;

    common::table_id_t getUpperRelTableID() const { return upperRelTableID; }
    common::table_id_t getLowerRelTableID() const { return lowerRelTableID; }
    const std::string& getIndexColumnName() const { return columnName; }
    common::offset_t getUpperEntryPoint() const { return upperEntryPoint; }
    common::offset_t getLowerEntryPoint() const { return lowerEntryPoint; }

private:
    common::table_id_t upperRelTableID;
    common::table_id_t lowerRelTableID;
    std::string columnName;
    common::offset_t upperEntryPoint;
    common::offset_t lowerEntryPoint;
};

} // namespace catalog
} // namespace kuzu
