#include "duckdb_connector.h"

#include "common/case_insensitive_map.h"
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

static std::string getDuckDBExtensionOptions(main::ClientContext* context, std::string optionName) {
    static common::case_insensitive_map_t<std::string> DUCKDB_OPTION_NAMES = {
        {httpfs::S3AccessKeyID::NAME, "KEY_ID"}, {httpfs::S3SecretAccessKey::NAME, "SECRET"},
        {httpfs::S3EndPoint::NAME, "ENDPOINT"}, {httpfs::S3URLStyle::NAME, "URL_STYLE"},
        {httpfs::S3Region::NAME, "REGION"}};
    auto optionNameInDuckDB = DUCKDB_OPTION_NAMES.at(optionName);
    if (!context->isOptionSet(optionName)) {
        return "";
    }
    auto optionValueInKuzu = context->getCurrentSetting(optionName).toString();
    return common::stringFormat("{} '{}',", optionNameInDuckDB, optionValueInKuzu);
}

void S3DuckDBConnector::connect(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* context) {
    // Creates an in-memory duckdb instance, then install httpfs and attach remote duckdb.
    instance = std::make_unique<duckdb::DuckDB>(nullptr);
    connection = std::make_unique<duckdb::Connection>(*instance);
    executeQuery("install httpfs;");
    executeQuery("load httpfs;");
    std::string templateQuery = R"(CREATE SECRET s3_secret (
        {}
        TYPE S3
    );)";
    std::string options = "";
    options += getDuckDBExtensionOptions(context, "s3_access_key_id");
    options += getDuckDBExtensionOptions(context, "s3_secret_access_key");
    options += getDuckDBExtensionOptions(context, "s3_region");
    options += getDuckDBExtensionOptions(context, "s3_url_style");
    options += getDuckDBExtensionOptions(context, "s3_endpoint");
    templateQuery = common::stringFormat(templateQuery, options);
    executeQuery(templateQuery);
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
