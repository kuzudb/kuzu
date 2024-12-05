#include "connector/duckdb_secret_manager.h"

#include "s3_download_options.h"

namespace kuzu {
namespace duckdb_extension {

static std::string getDuckDBExtensionOptions(main::ClientContext* context, std::string optionName) {
    static common::case_insensitive_map_t<std::string> DUCKDB_OPTION_NAMES = {
        {httpfs::S3AccessKeyID::NAME, "KEY_ID"}, {httpfs::S3SecretAccessKey::NAME, "SECRET"},
        {httpfs::S3EndPoint::NAME, "ENDPOINT"}, {httpfs::S3URLStyle::NAME, "URL_STYLE"},
        {httpfs::S3Region::NAME, "REGION"}};
    auto optionNameInDuckDB = DUCKDB_OPTION_NAMES.at(optionName);
    auto optionValueInKuzu = context->getCurrentSetting(optionName).toString();
    return common::stringFormat("{} '{}',", optionNameInDuckDB, optionValueInKuzu);
}

std::string DuckDBSecretManager::getS3Secret(main::ClientContext* context) {
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
    return common::stringFormat(templateQuery, options);
}

} // namespace duckdb_extension
} // namespace kuzu
