#include "duckdb_catalog.h"

namespace kuzu {
namespace duckdb_scanner {

common::table_id_t DuckDBCatalogContent::createForeignTable(
    const binder::BoundCreateTableInfo& info) {
    auto tableID = assignNextTableID();
    auto extraInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
        BoundExtraCreateDuckDBTableInfo*>(info.extraInfo.get());
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        columnNames.push_back(propertyInfo.name);
        columnTypes.push_back(propertyInfo.type);
    }
    DuckDBScanBindData bindData(common::stringFormat("SELECT * FROM {}", info.tableName),
        extraInfo->dbPath, std::move(columnTypes), std::move(columnNames));
    auto tableEntry = std::make_unique<catalog::DuckDBTableCatalogEntry>(
        info.tableName, tableID, getScanFunction(std::move(bindData)));
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        tableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    tables->createEntry(std::move(tableEntry));
    return tableID;
}

} // namespace duckdb_scanner
} // namespace kuzu
