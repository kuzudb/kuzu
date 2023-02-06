#include "common/utils.h"

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace kuzu {
namespace common {

std::shared_ptr<spdlog::logger> LoggerUtils::getOrCreateLogger(const std::string& loggerName) {
    std::shared_ptr<spdlog::logger> logger = spdlog::get(loggerName);
    if (!logger) {
        logger = spdlog::stdout_logger_mt(loggerName);
    }
    return logger;
}

std::vector<std::string> StringUtils::split(
    const std::string& input, const std::string& delimiter) {
    auto result = std::vector<std::string>();
    auto prevPos = 0u;
    auto currentPos = input.find(delimiter, prevPos);
    while (currentPos != std::string::npos) {
        result.push_back(input.substr(prevPos, currentPos - prevPos));
        prevPos = currentPos + 1;
        currentPos = input.find(delimiter, prevPos);
    }
    result.push_back(input.substr(prevPos));
    return result;
}

} // namespace common
} // namespace kuzu
