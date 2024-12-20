#include "connector/remote_duckdb_connector.h"

#include "common/case_insensitive_map.h"
#include "connector/duckdb_secret_manager.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

void HTTPDuckDBConnector::connect(const std::string& dbPath, const std::string& catalogName,
    const std::string& /*schemaName*/, main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    executeQuery(common::stringFormat("attach '{}' as {} (read_only);", dbPath, catalogName));
}

void S3DuckDBConnector::connect(const std::string& dbPath, const std::string& catalogName,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    executeQuery(DuckDBSecretManager::getS3Secret(context));
    executeQuery(common::stringFormat("attach '{}' as {} (read_only);", dbPath, catalogName));
}

} // namespace duckdb_extension
} // namespace kuzu
