#include "s3fs_config.h"

#include "extension/extension.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace httpfs {

using namespace common;

static constexpr std::array AUTH_OPTION_NAMES = {"ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "ENDPOINT",
    "URL_STYLE", "REGION"};

static constexpr std::array<std::string_view, 1> s3PrefixArray = {"s3://"};
S3FileSystemConfig getS3Config() {
    return S3FileSystemConfig{.prefixes = s3PrefixArray,
        .fsName = "S3",
        .accessKeyIDOption = {common::Value{""}},
        .secretAccessKeyOption = {common::Value{""}},
        .endpointOption = {common::Value{"s3.amazonaws.com"}},
        .urlStyleOption = {{common::Value{"vhost"}}},
        .regionOption = {common::Value{"us-east-1"}}};
}

// The S3 filesystem communicates with GCS using interoperability mode
// https://cloud.google.com/storage/docs/aws-simple-migration
static constexpr std::array<std::string_view, 2> gcsPrefixArray = {"gcs://", "gs://"};
S3FileSystemConfig getGCSConfig() {
    return S3FileSystemConfig{.prefixes = gcsPrefixArray,
        .fsName = "GCS",
        .accessKeyIDOption = {common::Value{""}},
        .secretAccessKeyOption = {common::Value{""}},
        .endpointOption = {common::Value{"storage.googleapis.com"}},
        .urlStyleOption = {{common::Value{"path"}}},
        .regionOption = {common::Value{"us-east-1"}}};
}

std::span<const S3FileSystemConfig> S3FileSystemConfig::getAvailableConfigs() {
    static const std::array ret = {getS3Config(), getGCSConfig()};
    return ret;
}

std::string getFSOptionName(const S3FileSystemConfig& config, std::string_view originalOption) {
    return stringFormat("{}_{}", config.fsName, originalOption);
}

static std::string getAuthParam(const S3FileSystemConfig& config, const S3AuthOption& entry,
    main::ClientContext* context, const std::string& optionName) {
    return entry.isConfigurable ?
               context->getCurrentSetting(getFSOptionName(config, optionName)).toString() :
               entry.defaultValue.toString();
}

static decltype(auto) getAuthOptionRefs(const S3FileSystemConfig& config) {
    return std::array<const S3AuthOption*, AUTH_OPTION_NAMES.size()>{&config.accessKeyIDOption,
        &config.secretAccessKeyOption, &config.endpointOption, &config.urlStyleOption,
        &config.regionOption};
}

S3AuthParams S3FileSystemConfig::getAuthParams(main::ClientContext* context) const {
    S3AuthParams authParams;
    authParams.accessKeyID = getAuthParam(*this, accessKeyIDOption, context, AUTH_OPTION_NAMES[0]);
    authParams.secretAccessKey =
        getAuthParam(*this, secretAccessKeyOption, context, AUTH_OPTION_NAMES[1]);
    authParams.endpoint = getAuthParam(*this, endpointOption, context, AUTH_OPTION_NAMES[2]);
    authParams.urlStyle = getAuthParam(*this, urlStyleOption, context, AUTH_OPTION_NAMES[3]);
    authParams.region = getAuthParam(*this, regionOption, context, AUTH_OPTION_NAMES[4]);
    return authParams;
}

void S3FileSystemConfig::registerExtensionOptions(main::Database* db) const {
    const auto authOptions = getAuthOptionRefs(*this);
    for (size_t i = 0; i < AUTH_OPTION_NAMES.size(); ++i) {
        static constexpr LogicalTypeID optionType = common::LogicalTypeID::STRING;
        if (authOptions[i]->isConfigurable) {
            db->addExtensionOption(getFSOptionName(*this, AUTH_OPTION_NAMES[i]), optionType,
                authOptions[i]->defaultValue);
        }
    }
}

void S3FileSystemConfig::setEnvValue(main::ClientContext* context) const {
    const auto authOptions = getAuthOptionRefs(*this);
    for (size_t i = 0; i < AUTH_OPTION_NAMES.size(); ++i) {
        if (authOptions[i]->isConfigurable) {
            const auto fsOptionName = getFSOptionName(*this, AUTH_OPTION_NAMES[i]);
            auto optionValueFromEnv = main::ClientContext::getEnvVariable(fsOptionName);
            if (optionValueFromEnv != "") {
                context->setExtensionOption(fsOptionName, Value::createValue(optionValueFromEnv));
            }
        }
    }
}

} // namespace httpfs
} // namespace kuzu
