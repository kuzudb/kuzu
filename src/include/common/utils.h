#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "exception.h"

using namespace std;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

class LoggerUtils {
public:
    // Note: create logger is not thread safe.
    static shared_ptr<spdlog::logger> getOrCreateLogger(const string& loggerName);
};

class StringUtils {

public:
    static vector<string> split(const string& input, const string& delimiter);

    static void toUpper(string& input) {
        transform(input.begin(), input.end(), input.begin(), ::toupper);
    }

    static void toLower(string& input) {
        transform(input.begin(), input.end(), input.begin(), ::tolower);
    }

    static bool CharacterIsSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
    }

    static bool CharacterIsDigit(char c) { return c >= '0' && c <= '9'; }
    template<typename... Args>
    static string string_format(const string& format, Args... args) {
        int size_s = snprintf(nullptr, 0, format.c_str(), args...) + 1; // Extra space for '\0'
        if (size_s <= 0) {
            throw Exception("Error during formatting.");
        }
        auto size = static_cast<size_t>(size_s);
        auto buf = make_unique<char[]>(size);
        snprintf(buf.get(), size, format.c_str(), args...);
        return string(buf.get(), buf.get() + size - 1); // We don't want the '\0' inside
    }

    static string getLongStringErrorMessage(const char* strToInsert, uint64_t maxAllowedStrSize) {
        return StringUtils::string_format(
            "Maximum length of strings is %d. Input string's length is %d.", maxAllowedStrSize,
            strlen(strToInsert), strToInsert);
    }
};

template<typename FROM, typename TO>
unique_ptr<TO> ku_static_unique_pointer_cast(unique_ptr<FROM>&& old) {
    return unique_ptr<TO>{static_cast<TO*>(old.release())};
}
template<class FROM, class TO>
shared_ptr<TO> ku_reinterpret_pointer_cast(const shared_ptr<FROM>& r) {
    return shared_ptr<TO>(r, reinterpret_cast<typename shared_ptr<TO>::element_type*>(r.get()));
}

class BitmaskUtils {

public:
    static inline uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        assert(numBits <= 64);
        return numBits == 64 ? UINT64_MAX : ((uint64_t)1 << numBits) - 1;
    }
};

class ThreadUtils {

public:
    static string getThreadIDString();
};

class HashTableUtils {

public:
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
};

} // namespace common
} // namespace kuzu
