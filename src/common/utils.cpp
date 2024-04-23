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
    KU_ASSERT(logger);
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
        KU_UNREACHABLE;
    }
    }
}

uint64_t nextPowerOfTwo(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

bool isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

template<>
bool integerFitsIn<int64_t>(int64_t) {
    return true;
}

template<>
bool integerFitsIn<int32_t>(int64_t val) {
    return val >= INT32_MIN && val <= INT32_MAX;
}

template<>
bool integerFitsIn<int16_t>(int64_t val) {
    return val >= INT16_MIN && val <= INT16_MAX;
}

template<>
bool integerFitsIn<int8_t>(int64_t val) {
    return val >= INT8_MIN && val <= INT8_MAX;
}

template<>
bool integerFitsIn<uint64_t>(int64_t val) {
    return val >= 0;
}

template<>
bool integerFitsIn<uint32_t>(int64_t val) {
    return val >= 0 && val <= UINT32_MAX;
}

template<>
bool integerFitsIn<uint16_t>(int64_t val) {
    return val >= 0 && val <= UINT16_MAX;
}

template<>
bool integerFitsIn<uint8_t>(int64_t val) {
    return val >= 0 && val <= UINT8_MAX;
}

} // namespace common
} // namespace kuzu
