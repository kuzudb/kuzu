#pragma once

#include "spdlog/spdlog.h"

namespace kuzu {
namespace common {

class LoggingLevelUtils {
public:
    static spdlog::level::level_enum convertStrToLevelEnum(std::string loggingLevel);

    static std::string convertLevelEnumToStr(spdlog::level::level_enum levelEnum);
};

} // namespace common
} // namespace kuzu
