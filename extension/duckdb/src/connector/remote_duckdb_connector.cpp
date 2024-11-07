#include "connector/remote_duckdb_connector.h"

#include "common/case_insensitive_map.h"
#include "main/client_context.h"
#include "s3_download_options.h"

namespace kuzu {
namespace duckdb_extension {

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
    options += getDuckDBExtensionOptions(context, httpfs::S3AccessKeyID::NAME);
    options += getDuckDBExtensionOptions(context, httpfs::S3SecretAccessKey::NAME);
    options += getDuckDBExtensionOptions(context, httpfs::S3Region::NAME);
    options += getDuckDBExtensionOptions(context, httpfs::S3URLStyle::NAME);
    options += getDuckDBExtensionOptions(context, httpfs::S3EndPoint::NAME);
    templateQuery = common::stringFormat(templateQuery, options);
    executeQuery(templateQuery);
    executeQuery(common::stringFormat("attach '{}' as {} (read_only);", dbPath, catalogName));
}

} // namespace duckdb_extension
} // namespace kuzu
