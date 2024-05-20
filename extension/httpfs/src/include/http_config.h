#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace httpfs {

struct HTTPConfig {
    explicit HTTPConfig(main::ClientContext* context);

    bool cacheInMemory;
};

struct HTTPCacheInMemoryConfig {
    static constexpr const char* HTTP_CACHE_IN_MEMORY_ENV_VAR = "HTTP_CACHE_IN_MEMORY";
    static constexpr const char* HTTP_CACHE_IN_MEMORY_OPTION = "http_cache_in_memory";
    static constexpr bool DEFAULT_CACHE_IN_MEMORY = false;
};

struct HTTPConfigEnvProvider {
    static void setOptionValue(main::ClientContext* context);
};

} // namespace httpfs
} // namespace kuzu
