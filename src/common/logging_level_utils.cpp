#include "common/logging_level_utils.h"

#include <string>

#include "common/exception/conversion.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "spdlog/common.h"

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
        throw ConversionException(stringFormat("Unsupported logging level: {}.", loggingLevel));
    }
}

} // namespace common
} // namespace kuzu
