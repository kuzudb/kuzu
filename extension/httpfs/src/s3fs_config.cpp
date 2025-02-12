#include "s3fs_config.h"

#include "extension/extension.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace httpfs {

using namespace common;

static constexpr std::array<std::string_view, 1> s3PrefixArray = {"s3://"};
constexpr S3FileSystemConfig getS3Config() {
    return S3FileSystemConfig{.prefixes = s3PrefixArray,
        .fsName = "S3",
        .defaultEndpoint = "s3.amazonaws.com",
        .defaultUrlStyle = "vhost"};
}

// The S3 filesystem communicates with GCS using interoperability mode
// https://cloud.google.com/storage/docs/aws-simple-migration
static constexpr std::array<std::string_view, 2> gcsPrefixArray = {"gcs://", "gs://"};
constexpr S3FileSystemConfig getGCSConfig() {
    return S3FileSystemConfig{.prefixes = gcsPrefixArray,
        .fsName = "GCS",
        .defaultEndpoint = "storage.googleapis.com",
        .defaultUrlStyle = "path"};
}

std::span<const S3FileSystemConfig> S3FileSystemConfig::getAvailableConfigs() {
    static constexpr std::array ret = {getS3Config(), getGCSConfig()};
    return ret;
}

std::string getFSOptionName(const S3FileSystemConfig& config, std::string_view originalOption) {
    return stringFormat("{}_{}", config.fsName, originalOption);
}

std::vector<std::string> S3FileSystemConfig::getAuthOptions() const {
    std::vector<std::string> ret;
    for (size_t i = 0; i < AUTH_OPTIONS.size(); ++i) {
        ret.push_back(getFSOptionName(*this, AUTH_OPTIONS[i]));
    }
    return ret;
}

S3AuthParams S3FileSystemConfig::getAuthParams(main::ClientContext* context) const {
    S3AuthParams authParams;
    authParams.accessKeyID =
        context->getCurrentSetting(getFSOptionName(*this, AUTH_OPTIONS[0])).toString();
    authParams.secretAccessKey =
        context->getCurrentSetting(getFSOptionName(*this, AUTH_OPTIONS[1])).toString();
    authParams.endpoint =
        context->getCurrentSetting(getFSOptionName(*this, AUTH_OPTIONS[2])).toString();
    authParams.urlStyle =
        context->getCurrentSetting(getFSOptionName(*this, AUTH_OPTIONS[3])).toString();
    authParams.region =
        context->getCurrentSetting(getFSOptionName(*this, AUTH_OPTIONS[4])).toString();
    return authParams;
}

void S3FileSystemConfig::registerExtensionOptions(main::Database* db) const {
    static constexpr LogicalTypeID optionType = common::LogicalTypeID::STRING;
    static constexpr auto defaultRegion = "us-east-1";
    std::array defaultValues = {common::Value{""}, common::Value{""},
        common::Value{std::string{defaultEndpoint}}, common::Value{std::string{defaultUrlStyle}},
        common::Value{defaultRegion}};
    static_assert(AUTH_OPTIONS.size() == defaultValues.size());
    for (size_t i = 0; i < AUTH_OPTIONS.size(); ++i) {
        db->addExtensionOption(getFSOptionName(*this, AUTH_OPTIONS[i]), optionType,
            std::move(defaultValues[i]));
    }
}

void S3FileSystemConfig::setEnvValue(main::ClientContext* context) const {
    for (const auto fsOption : AUTH_OPTIONS) {
        const auto fsOptionName = getFSOptionName(*this, fsOption);
        auto optionValueFromEnv = main::ClientContext::getEnvVariable(fsOptionName);
        if (optionValueFromEnv != "") {
            context->setExtensionOption(fsOptionName, Value::createValue(optionValueFromEnv));
        }
    }
}

} // namespace httpfs
} // namespace kuzu
