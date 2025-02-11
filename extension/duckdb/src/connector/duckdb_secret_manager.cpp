#include "connector/duckdb_secret_manager.h"

#include "remote_fs_config.h"

namespace kuzu {
namespace duckdb_extension {

static std::string getDuckDBExtensionOptions(main::ClientContext* context,
    std::span<const std::string> kuzuOptions) {
    std::string options = "";

    static constexpr std::array DUCKDB_OPTION_NAMES = {"KEY_ID", "SECRET", "ENDPOINT", "URL_STYLE",
        "REGION"};
    // the order should also be the same as httpfs::AUTH_OPTIONS defined in remote_fs_config.h
    KU_ASSERT(DUCKDB_OPTION_NAMES.size() == kuzuOptions.size());
    for (size_t i = 0; i < DUCKDB_OPTION_NAMES.size(); ++i) {
        auto optionNameInDuckDB = DUCKDB_OPTION_NAMES[i];
        auto optionValueInKuzu = context->getCurrentSetting(kuzuOptions[i]).toString();
        options += common::stringFormat("{} '{}',", optionNameInDuckDB, optionValueInKuzu);
    }

    return options;
}

std::string DuckDBSecretManager::getRemoteFSSecret(main::ClientContext* context,
    const httpfs::RemoteFSConfig& config) {
    KU_ASSERT(config.fsName == "S3" || config.fsName == "GCS");
    std::string templateQuery = R"(CREATE SECRET s3_secret (
        {}
        TYPE {}
    );)";
    return common::stringFormat(templateQuery,
        getDuckDBExtensionOptions(context, config.getAuthOptions()), config.fsName);
}

} // namespace duckdb_extension
} // namespace kuzu
