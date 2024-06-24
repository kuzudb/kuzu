#include "sqlite_catalog.h"

#include "common/exception/runtime.h"

namespace kuzu {
namespace sqlite_extension {

std::string SqliteCatalog::getDefaultSchemaName() const {
    return DEFAULT_SCHEMA_NAME;
}

std::unique_ptr<binder::BoundCreateTableInfo> SqliteCatalog::bindCreateTableInfo(
    duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
    const std::string& /*catalogName*/) {
    std::vector<binder::PropertyInfo> propertyInfos;
    if (!bindPropertyInfos(con, tableName, DEFAULT_CATALOG_NAME, propertyInfos)) {
        return nullptr;
    }
    auto extraCreateSqliteTableInfo = std::make_unique<BoundExtraCreateSqliteTableInfo>(dbPath,
        "" /* dbPath */, DEFAULT_CATALOG_NAME, getDefaultSchemaName(), std::move(propertyInfos));
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::FOREIGN, tableName,
        common::ConflictAction::ON_CONFLICT_THROW, std::move(extraCreateSqliteTableInfo));
}

static void executeQueryAndCheckErrMsg(duckdb::Connection& con, std::string query) {
    auto result = con.Query(query);
    if (result->HasError()) {
        throw common::RuntimeException(common::stringFormat(
            "Failed to execute query {}, due to: {}", query, result->GetError()));
    }
}

std::pair<duckdb::DuckDB, duckdb::Connection> SqliteCatalog::getConnection(
    const std::string& dbPath) const {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    executeQueryAndCheckErrMsg(con, "install sqlite;");
    executeQueryAndCheckErrMsg(con, "load sqlite;");
    executeQueryAndCheckErrMsg(con,
        common::stringFormat("attach '{}' as {} (TYPE sqlite);", dbPath, DEFAULT_CATALOG_NAME));
    return std::make_pair(std::move(db), std::move(con));
}

} // namespace sqlite_extension
} // namespace kuzu
