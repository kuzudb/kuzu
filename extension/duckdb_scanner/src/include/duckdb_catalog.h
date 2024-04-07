#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_content.h"
#include "duckdb_scan.h"

namespace kuzu {
namespace duckdb_scanner {

struct BoundExtraCreateDuckDBTableInfo : public binder::BoundExtraCreateTableInfo {
    std::string dbPath;
    std::string catalogName;
    std::string schemaName;

    BoundExtraCreateDuckDBTableInfo(std::string dbPath, std::string catalogName,
        std::string schemaName, std::vector<binder::PropertyInfo> propertyInfos)
        : BoundExtraCreateTableInfo{std::move(propertyInfos)}, dbPath{std::move(dbPath)},
          catalogName{std::move(catalogName)}, schemaName{std::move(schemaName)} {}
    BoundExtraCreateDuckDBTableInfo(const BoundExtraCreateDuckDBTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)}, dbPath{other.dbPath},
          catalogName{other.catalogName}, schemaName{other.schemaName} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateDuckDBTableInfo>(*this);
    }
};

class DuckDBCatalogContent : public catalog::CatalogContent {
public:
    DuckDBCatalogContent() : catalog::CatalogContent{nullptr /* vfs */} {}

    virtual void init(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context);

protected:
    bool bindPropertyInfos(duckdb::Connection& con, const std::string& tableName,
        const std::string& catalogName, std::vector<binder::PropertyInfo>& propertyInfos);

private:
    virtual std::unique_ptr<binder::BoundCreateTableInfo> bindCreateTableInfo(
        duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
        const std::string& catalogName);

    virtual std::string getDefaultSchemaName() const;

    virtual std::pair<duckdb::DuckDB, duckdb::Connection> getConnection(
        const std::string& dbPath) const;

private:
    void createForeignTable(duckdb::Connection& con, const std::string& tableName,
        const std::string& dbPath, const std::string& catalogName);
};

} // namespace duckdb_scanner
} // namespace kuzu
