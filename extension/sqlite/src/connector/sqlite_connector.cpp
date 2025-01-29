#include "connector/sqlite_connector.h"

#include "extension/extension.h"
#include "function/sqlite_scan.h"
#include "main/client_context.h"

namespace kuzu {
namespace sqlite_extension {

void SqliteConnector::connect(const std::string& dbPath, const std::string& catalogName,
    const std::string& /*schemaName*/, main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach SQLITE.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install sqlite;");
    executeQuery("load sqlite;");
    executeQuery(
        common::stringFormat("attach '{}' as {} (TYPE sqlite, read_only)", dbPath, catalogName));
}

std::shared_ptr<duckdb_extension::DuckDBScanBindData> SqliteConnector::getScanBindData(
    std::string query, const std::vector<common::LogicalType>& columnTypes,
    const std::vector<std::string>& columnNames) const {
    return std::make_shared<SqliteScanBindData>(std::move(query), columnTypes, columnNames, *this);
}

} // namespace sqlite_extension
} // namespace kuzu
