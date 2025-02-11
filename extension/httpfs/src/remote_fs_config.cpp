#include "remote_fs_config.h"

#include "extension/extension.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace httpfs {

using namespace common;

static constexpr std::array<std::string_view, 1> s3PrefixArray = {"s3://"};
constexpr RemoteFSConfig getS3Config() {
    return RemoteFSConfig{.prefixes = s3PrefixArray,
        .httpHeaderPrefix = "x-amz",
        .fsName = "S3",
        .defaultEndpoint = "s3.amazonaws.com",
        .defaultUrlStyle = "vhost"};
}

static constexpr std::array<std::string_view, 2> gcsPrefixArray = {"gcs://", "gs://"};
constexpr RemoteFSConfig getGCSConfig() {
    return RemoteFSConfig{.prefixes = gcsPrefixArray,
        .httpHeaderPrefix = "x-goog",
        .fsName = "GCS",
        .defaultEndpoint = "storage.googleapis.com",
        .defaultUrlStyle = "path"};
}

std::span<const RemoteFSConfig> RemoteFSConfig::getAvailableConfigs() {
    static constexpr std::array ret = {getS3Config(), getGCSConfig()};
    return ret;
}

std::string RemoteFSConfig::substituteHeaderPrefix(const std::string_view header) const {
    return stringFormat(header, httpHeaderPrefix);
}

std::string getFSOptionName(const RemoteFSConfig& config, std::string_view originalOption) {
    return stringFormat("{}_{}", config.fsName, originalOption);
}

std::vector<std::string> RemoteFSConfig::getAuthOptions() const {
    std::vector<std::string> ret;
    for (size_t i = 0; i < AUTH_OPTIONS.size(); ++i) {
        ret.push_back(getFSOptionName(*this, AUTH_OPTIONS[i]));
    }
    return ret;
}

void RemoteFSConfig::registerExtensionOptions(main::Database* db) const {
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

void RemoteFSConfig::setEnvValue(main::ClientContext* context) const {
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
