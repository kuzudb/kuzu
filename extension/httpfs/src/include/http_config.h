#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace httpfs_extension {

struct HTTPConfig {
    explicit HTTPConfig(main::ClientContext* context);

    bool cacheFile;
};

struct HTTPCacheFileConfig {
    static constexpr const char* HTTP_CACHE_FILE_ENV_VAR = "HTTP_CACHE_FILE";
    static constexpr const char* HTTP_CACHE_FILE_OPTION = "http_cache_file";
    static constexpr bool DEFAULT_CACHE_FILE = false;
};

struct HTTPConfigEnvProvider {
    static void setOptionValue(main::ClientContext* context);
};

} // namespace httpfs_extension
} // namespace kuzu
