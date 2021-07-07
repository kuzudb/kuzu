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

} // namespace common
} // namespace graphflow
