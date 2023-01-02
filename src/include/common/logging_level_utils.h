#pragma once

#include "spdlog/spdlog.h"

using namespace std;

namespace kuzu {
namespace common {

class LoggingLevelUtils {
public:
    static spdlog::level::level_enum convertStrToLevelEnum(string loggingLevel);

    static string convertLevelEnumToStr(spdlog::level::level_enum levelEnum);
};

} // namespace common
} // namespace kuzu
