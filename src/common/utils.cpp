#include "common/utils.h"

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace kuzu {
namespace common {

shared_ptr<spdlog::logger> LoggerUtils::getOrCreateLogger(const std::string& loggerName) {
    shared_ptr<spdlog::logger> logger = spdlog::get(loggerName);
    if (!logger) {
        logger = spdlog::stdout_logger_mt(loggerName);
    }
    return logger;
}

vector<string> StringUtils::split(const string& input, const string& delimiter) {
    auto result = vector<string>();
    auto prevPos = 0u;
    auto currentPos = input.find(delimiter, prevPos);
    while (currentPos != string::npos) {
        result.push_back(input.substr(prevPos, currentPos - prevPos));
        prevPos = currentPos + 1;
        currentPos = input.find(delimiter, prevPos);
    }
    result.push_back(input.substr(prevPos));
    return result;
}

string ThreadUtils::getThreadIDString() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

} // namespace common
} // namespace kuzu
