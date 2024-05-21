#include "http_config.h"

#include "function/cast/functions/cast_from_string_functions.h"

namespace kuzu {
namespace httpfs {

HTTPConfig::HTTPConfig(main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    if (!context->isOptionSet(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_OPTION)) {
        auto cacheInMemoryEnvVal =
            context->getEnvVariable(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_ENV_VAR);
        if (cacheInMemoryEnvVal != "") {
            bool cacheInMemory;
            function::CastString::operation(
                ku_string_t{cacheInMemoryEnvVal.c_str(), cacheInMemoryEnvVal.length()},
                cacheInMemory);
            context->setExtensionOption(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_OPTION,
                Value::createValue(cacheInMemory));
        }
    }
    cacheFile = context->getCurrentSetting(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_OPTION)
                    .getValue<bool>();
}

void HTTPConfigEnvProvider::setOptionValue(main::ClientContext* context) {
    auto cacheInMemoryStrVal =
        context->getEnvVariable(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_ENV_VAR);
    if (cacheInMemoryStrVal != "") {
        bool cacheInMemory;
        function::CastString::operation(
            ku_string_t{cacheInMemoryStrVal.c_str(), cacheInMemoryStrVal.length()}, cacheInMemory);
        context->setExtensionOption(HTTPCacheInMemoryConfig::HTTP_CACHE_FILE_OPTION,
            Value::createValue(cacheInMemory));
    }
}

} // namespace httpfs
} // namespace kuzu
