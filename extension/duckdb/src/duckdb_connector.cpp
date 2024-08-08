#include "duckdb_connector.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

static DuckDBConnectionType getConnectionType(const std::string& path) {
    if (path.rfind("https://", 0) == 0 || path.rfind("http://", 0) == 0) {
        return DuckDBConnectionType::HTTP;
    } else if (path.rfind("s3://", 0) == 0) {
        return DuckDBConnectionType::S3;
    } else {
        return DuckDBConnectionType::LOCAL;
    }
}

std::unique_ptr<duckdb::MaterializedQueryResult> DuckDBConnector::executeQuery(
    std::string query) const {
    KU_ASSERT(instance != nullptr && connection != nullptr);
    auto result = connection->Query(query);
    if (result->HasError()) {
        throw common::Exception{result->GetError()};
    }
    return result;
}

void LocalDuckDBConnector::connect(const std::string& dbPath, const std::string& /*catalogName*/,
    main::ClientContext* context) {
    if (!context->getVFSUnsafe()->fileOrPathExists(dbPath, context)) {
        throw common::RuntimeException{
            common::stringFormat("Given duckdb database path {} does not exist.", dbPath)};
    }
    duckdb::DBConfig dbConfig{true /* isReadOnly */};
    instance = std::make_unique<duckdb::DuckDB>(dbPath, &dbConfig);
    connection = std::make_unique<duckdb::Connection>(*instance);
}

void HTTPDuckDBConnector::connect(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* /*context*/) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    executeQuery(common::stringFormat("attach '{}' as {} (read_only);", dbPath, catalogName));
}

static void setDuckDBExtensionOptions(const DuckDBConnector& connector,
    main::ClientContext* context, std::string optionName) {
    auto optionNameInKuzu = common::stringFormat("duckdb_{}", optionName);
    if (!context->isOptionSet(optionNameInKuzu)) {
        return;
    }
    auto optionValueInKuzu = context->getCurrentSetting(optionNameInKuzu).toString();
    connector.executeQuery(
        common::stringFormat("set {}='{}';", std::move(optionName), std::move(optionValueInKuzu)));
}

void S3DuckDBConnector::connect(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    setDuckDBExtensionOptions(*this, context, "s3_access_key_id");
    setDuckDBExtensionOptions(*this, context, "s3_secret_access_key");
    setDuckDBExtensionOptions(*this, context, "s3_endpoint");
    setDuckDBExtensionOptions(*this, context, "s3_url_style");
    setDuckDBExtensionOptions(*this, context, "s3_region");
    executeQuery(common::stringFormat("attach '{}' as {} (read_only);", dbPath, catalogName));
}

std::unique_ptr<DuckDBConnector> DuckDBConnectorFactory::getDuckDBConnector(
    const std::string& dbPath) {
    auto type = getConnectionType(dbPath);
    switch (type) {
    case DuckDBConnectionType::LOCAL:
        return std::make_unique<LocalDuckDBConnector>();
    case DuckDBConnectionType::HTTP:
        return std::make_unique<HTTPDuckDBConnector>();
    case DuckDBConnectionType::S3:
        return std::make_unique<S3DuckDBConnector>();
    default:
        KU_UNREACHABLE;
    }
}

} // namespace duckdb_extension
} // namespace kuzu
