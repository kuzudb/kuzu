#pragma once

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "common/types/value/value.h"
#include <span>

namespace kuzu {
namespace main {
class Database;
class ClientContext;
} // namespace main

namespace httpfs_extension {

struct S3AuthParams {
    std::string accessKeyID;
    std::string secretAccessKey;
    std::string sessionToken;
    std::string endpoint;
    std::string urlStyle;
    std::string region;
};

struct S3AuthOption {
    common::Value defaultValue;
    bool isConfidential = false;
    bool isConfigurable = true;
};

struct S3FileSystemConfig {
    std::span<const std::string_view> prefixes;
    std::string_view fsName;
    S3AuthOption accessKeyIDOption;
    S3AuthOption secretAccessKeyOption;
    S3AuthOption sessionTokenOption;
    S3AuthOption endpointOption;
    S3AuthOption urlStyleOption;
    S3AuthOption regionOption;

    void registerExtensionOptions(main::Database* db) const;
    void setEnvValue(main::ClientContext* context) const;
    S3AuthParams getAuthParams(main::ClientContext* context) const;

    static std::span<const S3FileSystemConfig> getAvailableConfigs();
};

} // namespace httpfs_extension
} // namespace kuzu
