#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <span>

namespace kuzu {
namespace main {
class Database;
class ClientContext;
} // namespace main

namespace httpfs {

static constexpr std::array AUTH_OPTIONS = {"ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "ENDPOINT",
    "URL_STYLE", "REGION"};

struct AuthParams {
    std::string accessKeyID;
    std::string secretAccessKey;
    std::string endpoint;
    std::string urlStyle;
    std::string region;
};

struct RemoteFSConfig {
    std::span<const std::string_view> prefixes;
    std::string_view httpHeaderPrefix;
    std::string_view fsName;
    std::string_view defaultEndpoint;
    std::string_view defaultUrlStyle;

    std::string substituteHeaderPrefix(const std::string_view header) const;
    void registerExtensionOptions(main::Database* db) const;
    void setEnvValue(main::ClientContext* context) const;
    std::vector<std::string> getAuthOptions() const;
    AuthParams getAuthParams(main::ClientContext* context) const;

    static std::span<const RemoteFSConfig> getAvailableConfigs();
};

} // namespace httpfs
} // namespace kuzu
