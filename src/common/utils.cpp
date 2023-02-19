#include "common/utils.h"

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace kuzu {
namespace common {

void LoggerUtils::createLogger(LoggerConstants::LoggerEnum loggerEnum) {
    auto loggerName = getLoggerName(loggerEnum);
    if (!spdlog::get(loggerName)) {
        spdlog::stdout_logger_mt(loggerName);
    }
}

std::shared_ptr<spdlog::logger> LoggerUtils::getLogger(LoggerConstants::LoggerEnum loggerEnum) {
    auto loggerName = getLoggerName(loggerEnum);
    std::shared_ptr<spdlog::logger> logger = spdlog::get(loggerName);
    assert(logger);
    return logger;
}

void LoggerUtils::dropLogger(LoggerConstants::LoggerEnum loggerEnum) {
    auto loggerName = getLoggerName(loggerEnum);
    spdlog::drop(loggerName);
}

std::string LoggerUtils::getLoggerName(LoggerConstants::LoggerEnum loggerEnum) {
    switch (loggerEnum) {
    case LoggerConstants::LoggerEnum::DATABASE: {
        return "database";
    } break;
    case LoggerConstants::LoggerEnum::CSV_READER: {
        return "csv_reader";
    } break;
    case LoggerConstants::LoggerEnum::LOADER: {
        return "loader";
    } break;
    case LoggerConstants::LoggerEnum::PROCESSOR: {
        return "processor";
    } break;
    case LoggerConstants::LoggerEnum::BUFFER_MANAGER: {
        return "buffer_manager";
    } break;
    case LoggerConstants::LoggerEnum::CATALOG: {
        return "catalog";
    } break;
    case LoggerConstants::LoggerEnum::STORAGE: {
        return "storage";
    } break;
    case LoggerConstants::LoggerEnum::TRANSACTION_MANAGER: {
        return "transaction_manager";
    } break;
    case LoggerConstants::LoggerEnum::WAL: {
        return "wal";
    } break;
    default: {
        assert(false);
    }
    }
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
