#include "connector/unity_catalog_connector.h"

#include "options/unity_catalog_options.h"

namespace kuzu {
namespace unity_catalog_extension {

void UnityCatalogConnector::connect(const std::string& dbPath, const std::string& catalogName,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach postgres.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install uc_catalog from core_nightly;");
    executeQuery("load uc_catalog;");
    executeQuery("install delta;");
    executeQuery("load delta;");
    executeQuery("install delta;");
    executeQuery(DuckDBUnityCatalogSecretManager::getSecret(context));
    executeQuery(common::stringFormat("attach '{}' as {} (TYPE UC_CATALOG, read_only);", dbPath,
        catalogName));
}

} // namespace unity_catalog_extension
} // namespace kuzu
