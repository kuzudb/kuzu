#include "http_config.h"

#include "function/cast/functions/cast_from_string_functions.h"

namespace kuzu {
namespace httpfs {

HTTPConfig::HTTPConfig(main::ClientContext* context) {
    assert(context != nullptr);
    if (!context->isOptionSet(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_OPTION)) {
        auto cacheInMemoryStrVal =
            context->getEnvVariable(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_ENV_VAR);
        if (cacheInMemoryStrVal != "") {
            bool cacheInMemory_;
            function::CastString::operation(
                ku_string_t{cacheInMemoryStrVal.c_str(), cacheInMemoryStrVal.length()},
                cacheInMemory_);
            context->setExtensionOption(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_OPTION,
                Value::createValue(cacheInMemory_));
        }
    }
    cacheInMemory = context->getCurrentSetting(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_OPTION)
                        .getValue<bool>();
}

void HTTPConfigEnvProvider::setOptionValue(main::ClientContext* context) {
    auto cacheInMemoryStrVal =
        context->getEnvVariable(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_ENV_VAR);
    if (cacheInMemoryStrVal != "") {
        bool cacheInMemory;
        function::CastString::operation(
            ku_string_t{cacheInMemoryStrVal.c_str(), cacheInMemoryStrVal.length()}, cacheInMemory);
        context->setExtensionOption(HTTPCacheInMemoryConfig::HTTP_CACHE_IN_MEMORY_OPTION,
            Value::createValue(cacheInMemory));
    }
}

} // namespace httpfs
} // namespace kuzu
