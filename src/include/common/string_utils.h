#pragma once

#include <algorithm>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "spdlog/fmt/fmt.h"

namespace kuzu {
namespace common {

class StringUtils {

public:
    template<typename... Args>
    inline static std::string string_format(const std::string& format, Args... args) {
        return fmt::format(fmt::runtime(format), args...);
    }

    static std::vector<std::string> split(
        const std::string& input, const std::string& delimiter, bool ignoreEmptyStringParts = true);

    static std::vector<std::string> splitByAnySpace(const std::string& input);

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

    static inline std::string ltrim(const std::string& input) {
        auto s = input;
        s.erase(
            s.begin(), find_if(s.begin(), s.end(), [](unsigned char ch) { return !isspace(ch); }));
        return s;
    }
};

} // namespace common
} // namespace kuzu
