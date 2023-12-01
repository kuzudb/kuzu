#pragma once

#include "spdlog/spdlog.h" // IWYU pragma: keep: this is the public header.

namespace kuzu {
namespace common {

class LoggingLevelUtils {
public:
    static spdlog::level::level_enum convertStrToLevelEnum(std::string loggingLevel);
};

} // namespace common
} // namespace kuzu
