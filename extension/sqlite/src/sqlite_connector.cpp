#include "sqlite_connector.h"

namespace kuzu {
namespace sqlite_extension {

void SqliteConnector::connect(const std::string& dbPath, main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install sqlite;");
    executeQuery("load postgres;");
    executeQuery(common::stringFormat("attach '{}' (TYPE sqlite, read_only)", dbPath));
}

} // namespace sqlite_extension
} // namespace kuzu
