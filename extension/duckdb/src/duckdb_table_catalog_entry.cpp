#include "duckdb_table_catalog_entry.h"

#include "binder/ddl/bound_create_table_info.h"

namespace kuzu {
namespace catalog {

DuckDBTableCatalogEntry::DuckDBTableCatalogEntry(CatalogSet* set, std::string name,
    common::table_id_t tableID, function::TableFunction scanFunction)
    : TableCatalogEntry{set, CatalogEntryType::FOREIGN_TABLE_ENTRY, std::move(name), tableID},
      scanFunction{std::move(scanFunction)} {}

common::TableType DuckDBTableCatalogEntry::getTableType() const {
    return common::TableType::EXTERNAL;
}

std::unique_ptr<TableCatalogEntry> DuckDBTableCatalogEntry::copy() const {
    auto other = std::make_unique<DuckDBTableCatalogEntry>(set, name, tableID, scanFunction);
    other->copyFrom(*this);
    return other;
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
DuckDBTableCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction*) const {
    KU_UNREACHABLE;
}

} // namespace catalog
} // namespace kuzu
