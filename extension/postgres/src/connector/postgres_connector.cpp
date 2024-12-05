#include "connector/postgres_connector.h"

namespace kuzu {
namespace postgres_extension {

void PostgresConnector::connect(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach postgres.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install postgres;");
    executeQuery("load postgres;");
    executeQuery(
        common::stringFormat("attach '{}' as {} (TYPE postgres, read_only);", dbPath, catalogName));
}

} // namespace postgres_extension
} // namespace kuzu