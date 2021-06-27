#pragma once

#include "robin_hood.h"

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

class StringUtils {

public:
    static vector<string> split(const string& input, const string& delimiter);

    static bool CharacterIsSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
    }

    //    static bool CharacterIsNewline(char c) {
    //        return c == '\n' || c == '\r';
    //    }
    //
    static bool CharacterIsDigit(char c) { return c >= '0' && c <= '9'; }
    template<typename... Args>
    static std::string string_format(const std::string& format, Args... args) {
        int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) + 1; // Extra space for '\0'
        if (size_s <= 0) {
            throw std::runtime_error("Error during formatting.");
        }
        auto size = static_cast<size_t>(size_s);
        auto buf = std::make_unique<char[]>(size);
        std::snprintf(buf.get(), size, format.c_str(), args...);
        return std::string(buf.get(), buf.get() + size - 1); // We don't want the '\0' inside
    }
};

} // namespace common
} // namespace graphflow
