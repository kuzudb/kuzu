#include "connector/duckdb_secret_manager.h"

#include "s3fs_config.h"

namespace kuzu {
namespace duckdb_extension {

static std::string getDuckDBExtensionOptions(httpfs_extension::S3AuthParams kuzuOptions) {
    std::string options = "";
    options.append(common::stringFormat("KEY_ID '{}',", kuzuOptions.accessKeyID));
    options.append(common::stringFormat("SECRET '{}',", kuzuOptions.secretAccessKey));
    options.append(common::stringFormat("ENDPOINT '{}',", kuzuOptions.endpoint));
    options.append(common::stringFormat("URL_STYLE '{}',", kuzuOptions.urlStyle));
    options.append(common::stringFormat("REGION '{}',", kuzuOptions.region));
    return options;
}

std::string DuckDBSecretManager::getRemoteS3FSSecret(main::ClientContext* context,
    const httpfs_extension::S3FileSystemConfig& config) {
    KU_ASSERT(config.fsName == "S3" || config.fsName == "GCS");
    std::string templateQuery = R"(CREATE SECRET {}_secret (
        {}
        TYPE {}
    );)";
    return common::stringFormat(templateQuery, config.fsName,
        getDuckDBExtensionOptions(config.getAuthParams(context)), config.fsName);
}

} // namespace duckdb_extension
} // namespace kuzu
