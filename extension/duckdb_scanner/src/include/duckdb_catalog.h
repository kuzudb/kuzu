#pragma once

#include "catalog/catalog_content.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "duckdb_scan.h"
#include "duckdb_table_catalog_entry.h"

namespace kuzu {
namespace duckdb_scanner {

struct BoundExtraCreateDuckDBTableInfo final : public binder::BoundExtraCreateTableInfo {
    std::string dbPath;

    BoundExtraCreateDuckDBTableInfo(
        std::string dbPath, std::vector<binder::PropertyInfo> propertyInfos)
        : BoundExtraCreateTableInfo{std::move(propertyInfos)}, dbPath{std::move(dbPath)} {}
    BoundExtraCreateDuckDBTableInfo(const BoundExtraCreateDuckDBTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)}, dbPath{other.dbPath} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateDuckDBTableInfo>(*this);
    }
};

class DuckDBCatalogContent : public catalog::CatalogContent {
public:
    DuckDBCatalogContent() : catalog::CatalogContent{nullptr /* vfs */} {}

    common::table_id_t createForeignTable(const binder::BoundCreateTableInfo& info);
};

} // namespace duckdb_scanner
} // namespace kuzu
