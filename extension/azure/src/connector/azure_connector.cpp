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
    std::string query = "CREATE SECRET azure_secret (TYPE azure";
    for (auto [fieldName, _] : AzureConfig::getDefault().getFields()) {
        std::string duckdbFieldName = fieldName.substr(6); // strip "AZURE_" prefix
        query += common::stringFormat(", {} '{}'", duckdbFieldName,
            context->getCurrentSetting(fieldName).toString());
    }
    executeQuery(query + ");");
}

} // namespace azure_extension
} // namespace kuzu
