#pragma once

#include "duckdb_catalog.h"

namespace kuzu {
namespace sqlite_extension {

struct BoundExtraCreateSqliteTableInfo final
    : public duckdb_extension::BoundExtraCreateDuckDBTableInfo {
    std::string pgConnectionStr;

    BoundExtraCreateSqliteTableInfo(std::string pgConnectionStr, std::string dbPath,
        std::string catalogName, std::string schemaName,
        std::vector<binder::PropertyInfo> propertyInfos)
        : BoundExtraCreateDuckDBTableInfo{std::move(dbPath), std::move(catalogName),
              std::move(schemaName), std::move(propertyInfos)},
          pgConnectionStr{std::move(pgConnectionStr)} {}
    BoundExtraCreateSqliteTableInfo(const BoundExtraCreateSqliteTableInfo& other)
        : BoundExtraCreateDuckDBTableInfo{other.dbPath, other.catalogName, other.schemaName,
              copyVector(other.propertyInfos)},
          pgConnectionStr{other.pgConnectionStr} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateSqliteTableInfo>(*this);
    }
};

class SqliteCatalog final : public duckdb_extension::DuckDBCatalog {
public:
    SqliteCatalog(std::string dbPath, std::string /*catalogName*/, main::ClientContext* context,
        const binder::AttachOption& attachOption)
        : duckdb_extension::DuckDBCatalog{std::move(dbPath), DEFAULT_CATALOG_NAME, context,
              attachOption} {}

private:
    std::unique_ptr<binder::BoundCreateTableInfo> bindCreateTableInfo(duckdb::Connection& con,
        const std::string& tableName, const std::string& dbPath,
        const std::string& /*catalogName*/) override;

    std::string getDefaultSchemaName() const override;

    std::pair<duckdb::DuckDB, duckdb::Connection> getConnection(
        const std::string& dbPath) const override;

private:
    static constexpr char DEFAULT_CATALOG_NAME[] = "sqlite";
    static constexpr char DEFAULT_SCHEMA_NAME[] = "main";
};

} // namespace sqlite_extension
} // namespace kuzu
