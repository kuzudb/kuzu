#include "postgres_catalog.h"

#include "common/exception/runtime.h"

namespace kuzu {
namespace postgres_extension {

std::string PostgresCatalog::getDefaultSchemaName() const {
    return DEFAULT_SCHEMA_NAME;
}

std::unique_ptr<binder::BoundCreateTableInfo> PostgresCatalog::bindCreateTableInfo(
    duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
    const std::string& /*catalogName*/) {
    std::vector<binder::PropertyInfo> propertyInfos;
    if (!bindPropertyInfos(con, tableName, DEFAULT_CATALOG_NAME, propertyInfos)) {
        return nullptr;
    }
    auto extraCreatePostgresTableInfo = std::make_unique<BoundExtraCreatePostgresTableInfo>(dbPath,
        "" /* dbPath */, DEFAULT_CATALOG_NAME, getDefaultSchemaName(), std::move(propertyInfos));
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::EXTERNAL, tableName,
        common::ConflictAction::ON_CONFLICT_THROW, std::move(extraCreatePostgresTableInfo));
}

static void executeQueryAndCheckErrMsg(duckdb::Connection& con, std::string query) {
    auto result = con.Query(query);
    if (result->HasError()) {
        throw common::RuntimeException(common::stringFormat(
            "Failed to execute query {}, due to: {}", query, result->GetError()));
    }
}

std::pair<duckdb::DuckDB, duckdb::Connection> PostgresCatalog::getConnection(
    const std::string& dbPath) const {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    executeQueryAndCheckErrMsg(con, "install postgres;");
    executeQueryAndCheckErrMsg(con, "load postgres;");
    executeQueryAndCheckErrMsg(con,
        common::stringFormat("attach '{}' as {} (TYPE postgres);", dbPath, DEFAULT_CATALOG_NAME));
    return std::make_pair(std::move(db), std::move(con));
}

} // namespace postgres_extension
} // namespace kuzu
