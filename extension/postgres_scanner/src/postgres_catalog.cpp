#include "postgres_catalog.h"

#include "common/exception/binder.h"
#include "postgres_storage.h"

namespace kuzu {
namespace postgres_scanner {

void PostgresCatalogContent::init(
    const std::string& dbPath, const std::string& /*catalogName*/, main::ClientContext* context) {
    duckdb_scanner::DuckDBCatalogContent::init(dbPath, DEFAULT_CATALOG_NAME, context);
}

std::string PostgresCatalogContent::getDefaultSchemaName() const {
    return DEFAULT_SCHEMA_NAME;
}

std::unique_ptr<binder::BoundCreateTableInfo> PostgresCatalogContent::bindCreateTableInfo(
    duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
    const std::string& /*catalogName*/) {
    std::vector<binder::PropertyInfo> propertyInfos;
    if (!bindPropertyInfos(con, tableName, DEFAULT_CATALOG_NAME, propertyInfos)) {
        return nullptr;
    }
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::FOREIGN, tableName,
        std::make_unique<BoundExtraCreatePostgresTableInfo>(dbPath, "memory", DEFAULT_CATALOG_NAME,
            getDefaultSchemaName(), std::move(propertyInfos)));
}

duckdb::Connection PostgresCatalogContent::getConnection(const std::string& dbPath) const {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("install postgres;");
    con.Query("load postgres;");
    auto result = con.Query(common::stringFormat("attach '{}' as {} (TYPE postgres);", dbPath,
        PostgresStorageExtension::POSTGRES_CATALOG_NAME_IN_DUCKDB));
    if (result->HasError()) {
        throw common::BinderException(common::stringFormat(
            "Failed to attach postgres database due to: {}", result->GetError()));
    }
    return con;
}

} // namespace postgres_scanner
} // namespace kuzu
