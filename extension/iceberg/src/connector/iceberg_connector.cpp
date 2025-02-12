#include "connector/iceberg_connector.h"

#include "main/duckdb_extension.h"

namespace kuzu {
namespace iceberg_extension {

void IcebergConnector::connect(const std::string& /*dbPath*/, const std::string& /*catalogName*/,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach postgres.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    // Install the Desired Extension on DuckDB
    executeQuery("install iceberg;");
    executeQuery("load iceberg;");
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    duckdb_extension::DuckDBExtension::initRemoteFSSecrets(*this, context);
}

} // namespace iceberg_extension
} // namespace kuzu
