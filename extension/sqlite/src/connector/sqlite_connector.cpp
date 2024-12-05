#include "connector/sqlite_connector.h"

namespace kuzu {
namespace sqlite_extension {

void SqliteConnector::connect(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach SQLITE.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install sqlite;");
    executeQuery("load sqlite;");
    executeQuery(
        common::stringFormat("attach '{}' as {} (TYPE sqlite, read_only)", dbPath, catalogName));
}

} // namespace sqlite_extension
} // namespace kuzu
