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
#include "spdlog/fmt/fmt.h"

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

class StringUtils {

public:
    template<typename... Args>
    inline static std::string string_format(const std::string& format, Args... args) {
        return fmt::format(fmt::runtime(format), args...);
    }

    static std::vector<std::string> split(const std::string& input, const std::string& delimiter);

    static void toUpper(std::string& input) {
        std::transform(input.begin(), input.end(), input.begin(), ::toupper);
    }

    static void toLower(std::string& input) {
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
    }

    static bool CharacterIsSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
    }

    static bool CharacterIsDigit(char c) { return c >= '0' && c <= '9'; }

    static std::string getLongStringErrorMessage(
        const char* strToInsert, uint64_t maxAllowedStrSize) {
        return string_format("Maximum length of strings is {}. Input string's length is {}.",
            maxAllowedStrSize, strlen(strToInsert), strToInsert);
    }
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

} // namespace common
} // namespace kuzu
