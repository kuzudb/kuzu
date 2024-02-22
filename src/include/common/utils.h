#pragma once

#include <memory>
#include <string>
#include <vector>

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
    static uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        KU_ASSERT(numBits <= 64);
        return numBits == 64 ? UINT64_MAX : ((uint64_t)1 << numBits) - 1;
    }
};

uint64_t nextPowerOfTwo(uint64_t v);

bool isLittleEndian();

template<typename T>
std::vector<T> copyVector(const std::vector<T>& objects) {
    std::vector<T> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        result.push_back(object->copy());
    }
    return result;
}

} // namespace common
} // namespace kuzu
