#include "duckdb_table_catalog_entry.h"

namespace kuzu {
namespace catalog {

DuckDBTableCatalogEntry::DuckDBTableCatalogEntry(std::string name, common::table_id_t tableID,
    function::TableFunction scanFunction)
    : TableCatalogEntry{CatalogEntryType::FOREIGN_TABLE_ENTRY, std::move(name), tableID},
      scanFunction{std::move(scanFunction)} {}

common::TableType DuckDBTableCatalogEntry::getTableType() const {
    return common::TableType::FOREIGN;
}

std::unique_ptr<CatalogEntry> DuckDBTableCatalogEntry::copy() const {
    return std::make_unique<DuckDBTableCatalogEntry>(*this);
}

} // namespace catalog
} // namespace kuzu
