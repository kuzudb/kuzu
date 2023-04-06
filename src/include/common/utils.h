#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/constants.h"
#include "exception.h"

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

template<typename FROM, typename TO>
std::unique_ptr<TO> ku_static_unique_pointer_cast(std::unique_ptr<FROM>&& old) {
    return std::unique_ptr<TO>{static_cast<TO*>(old.release())};
}
template<class FROM, class TO>
std::shared_ptr<TO> ku_reinterpret_pointer_cast(const std::shared_ptr<FROM>& r) {
    return std::shared_ptr<TO>(
        r, reinterpret_cast<typename std::shared_ptr<TO>::element_type*>(r.get()));
}

class BitmaskUtils {

public:
    static inline uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        assert(numBits <= 64);
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
