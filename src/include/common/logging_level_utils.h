#pragma once

#include "spdlog/spdlog.h"

namespace kuzu {
namespace common {

class LoggingLevelUtils {
public:
    static spdlog::level::level_enum convertStrToLevelEnum(std::string loggingLevel);
};

} // namespace common
} // namespace kuzu
