#include "common/logging_level_utils.h"

#include "common/utils.h"

namespace kuzu {
namespace common {

spdlog::level::level_enum LoggingLevelUtils::convertStrToLevelEnum(string loggingLevel) {
    StringUtils::toLower(loggingLevel);
    if (loggingLevel == "info") {
        return spdlog::level::level_enum::info;
    } else if (loggingLevel == "debug") {
        return spdlog::level::level_enum::debug;
    } else if (loggingLevel == "err") {
        return spdlog::level::level_enum::err;
    }
    throw ConversionException(
        StringUtils::string_format("Unsupported logging level: %s.", loggingLevel.c_str()));
}

string LoggingLevelUtils::convertLevelEnumToStr(spdlog::level::level_enum levelEnum) {
    switch (levelEnum) {
    case spdlog::level::level_enum::trace: {
        return "trace";
    }
    case spdlog::level::level_enum::debug: {
        return "debug";
    }
    case spdlog::level::level_enum::info: {
        return "info";
    }
    case spdlog::level::level_enum::warn: {
        return "warn";
    }
    case spdlog::level::level_enum::err: {
        return "err";
    }
    case spdlog::level::level_enum::critical: {
        return "critical";
    }
    case spdlog::level::level_enum::off: {
        return "off";
    }
    default:
        throw ConversionException("Unsupported logging level conversion to string.");
    }
}

} // namespace common
} // namespace kuzu
