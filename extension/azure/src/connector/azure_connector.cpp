#include "connector/azure_connector.h"

#include "connector/azure_config.h"
#include "main/client_context.h"

namespace kuzu {
namespace azure_extension {

void AzureConnector::connect(const std::string& /*dbPath*/, const std::string& /*catalogName*/,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    // Creates an in-memory duckdb instance
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    // Install the Desired Extension on DuckDB
    executeQuery("install azure;");
    executeQuery("load azure;");
    initRemoteAzureSecrets(context);
}

void AzureConnector::initRemoteAzureSecrets(main::ClientContext* context) const {
    std::string query = R"(CREATE SECRET azure_secret (
        TYPE azure,
        CONNECTION_STRING '{}'
    );)";
    executeQuery(common::stringFormat(query,
        context->getCurrentSetting("AZURE_CONNECTION_STRING").toString()));
}

} // namespace azure_extension
} // namespace kuzu
