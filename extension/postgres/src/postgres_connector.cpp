#include "postgres_connector.h"

namespace kuzu {
namespace postgres_extension {

void PostgresConnector::connect(const std::string& dbPath, main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install postgres;");
    executeQuery("load postgres;");
    executeQuery(common::stringFormat("attach '{}' (TYPE postgres, read_only)", dbPath));
}

} // namespace postgres_extension
} // namespace kuzu
