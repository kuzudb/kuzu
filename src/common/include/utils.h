#pragma once

#include "robin_hood.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

using namespace std;

namespace graphflow {
namespace common {

// C-like string equalTo.
struct charArrayEqualTo {
    bool operator()(const char* lhs, const char* rhs) const { return strcmp(lhs, rhs) == 0; }
};

// C-like string hasher.
struct charArrayHasher {
    size_t operator()(const char* key) const { return robin_hood::hash_bytes(key, strlen(key)); }
};

class LoggerUtils {

public:
    static shared_ptr<spdlog::logger> getOrCreateSpdLogger(string loggerName) {
        shared_ptr<spdlog::logger> logger = spdlog::get(loggerName);
        if (!logger) {
            logger = spdlog::stdout_logger_mt(loggerName);
        }
        return logger;
    }
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
            throw runtime_error("Error during formatting.");
        }
        auto size = static_cast<size_t>(size_s);
        auto buf = make_unique<char[]>(size);
        snprintf(buf.get(), size, format.c_str(), args...);
        return string(buf.get(), buf.get() + size - 1); // We don't want the '\0' inside
    }
};

template<typename FROM, typename TO>
unique_ptr<TO> static_unique_pointer_cast(unique_ptr<FROM>&& old) {
    return unique_ptr<TO>{static_cast<TO*>(old.release())};
}

} // namespace common
} // namespace graphflow
