#include "connector/delta_connector.h"

#include "connector/duckdb_secret_manager.h"

namespace kuzu {
namespace delta_extension {

void DeltaConnector::connect(const std::string& /*dbPath*/, const std::string& /*catalogName*/,
    main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach postgres.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    // Install the Desired Extension on DuckDB
    if (format == "DELTA") {
        executeQuery("install delta;");
        executeQuery("load delta;");
    } else if (format == "ICEBERG") {
        executeQuery("install iceberg;");
        executeQuery("load iceberg;");
    }
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    executeQuery(duckdb_extension::DuckDBSecretManager::getS3Secret(context));
}

} // namespace delta_extension
} // namespace kuzu
