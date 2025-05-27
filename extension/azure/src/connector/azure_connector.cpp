#include "connector/azure_connector.h"

namespace kuzu {
namespace azure_extension {

void AzureConnector::connect(const std::string& /*dbPath*/, const std::string& /*catalogName*/,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach postgres.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    // Install the Desired Extension on DuckDB
    executeQuery("install azure;");
    executeQuery("load azure;");
    // initRemoteFSSecrets(context);
    initRemoteAzureFSSecrets();
}

} // namespace azure_extension
} // namespace kuzu
