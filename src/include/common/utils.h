#pragma once

#include <memory>
#include <string>

#include "common/assert.h"
#include "common/constants.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

class LoggerUtils {
public:
    // Note: create logger is not thread safe.
    static void createLogger(LoggerConstants::LoggerEnum loggerEnum);
    static std::shared_ptr<spdlog::logger> getLogger(LoggerConstants::LoggerEnum loggerEnum);
    static void dropLogger(LoggerConstants::LoggerEnum loggerEnum);

private:
    static std::string getLoggerName(LoggerConstants::LoggerEnum loggerEnum);
};

class BitmaskUtils {

public:
    static inline uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        KU_ASSERT(numBits <= 64);
        return numBits == 64 ? UINT64_MAX : ((uint64_t)1 << numBits) - 1;
    }
};

static uint64_t nextPowerOfTwo(uint64_t v) {
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

static bool isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

} // namespace common
} // namespace kuzu
