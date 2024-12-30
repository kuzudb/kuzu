#include "http_config.h"

#include "function/cast/functions/cast_from_string_functions.h"
#include "main/db_config.h"

namespace kuzu {
namespace httpfs {

HTTPConfig::HTTPConfig(main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    cacheFile =
        context->getCurrentSetting(HTTPCacheFileConfig::HTTP_CACHE_FILE_OPTION).getValue<bool>();
}

void HTTPConfigEnvProvider::setOptionValue(main::ClientContext* context) {
    const auto cacheFileOptionStrVal =
        main::ClientContext::getEnvVariable(HTTPCacheFileConfig::HTTP_CACHE_FILE_ENV_VAR);
    if (cacheFileOptionStrVal != "") {
        bool enableCacheFile = false;
        function::CastString::operation(
            ku_string_t{cacheFileOptionStrVal.c_str(), cacheFileOptionStrVal.length()},
            enableCacheFile);
        if (enableCacheFile && main::DBConfig::isDBPathInMemory(context->getDatabasePath())) {
            throw Exception("Cannot enable HTTP file cache when database is in memory");
        }
        context->setExtensionOption(HTTPCacheFileConfig::HTTP_CACHE_FILE_OPTION,
            Value::createValue(enableCacheFile));
    }
}

} // namespace httpfs
} // namespace kuzu
