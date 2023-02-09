#include "common/logging_level_utils.h"

#include "common/utils.h"

namespace kuzu {
namespace common {

spdlog::level::level_enum LoggingLevelUtils::convertStrToLevelEnum(std::string loggingLevel) {
    StringUtils::toLower(loggingLevel);
    if (loggingLevel == "info") {
        return spdlog::level::level_enum::info;
    } else if (loggingLevel == "debug") {
        return spdlog::level::level_enum::debug;
    } else if (loggingLevel == "err") {
        return spdlog::level::level_enum::err;
    } else {
        throw ConversionException(
            StringUtils::string_format("Unsupported logging level: %s.", loggingLevel.c_str()));
    }
}

} // namespace common
} // namespace kuzu
