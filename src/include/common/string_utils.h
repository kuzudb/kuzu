#pragma once

#include <algorithm>
#include <cstring>
#include <regex>
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

    static std::vector<std::string> splitBySpace(const std::string& input);

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

    static inline std::string rtrim(const std::string& input) {
        auto s = input;
        s.erase(find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !isspace(ch); }).base(),
            s.end());
        return s;
    }

    static inline void removeWhiteSpaces(std::string& str) {
        std::regex whiteSpacePattern{"\\s"};
        str = std::regex_replace(str, whiteSpacePattern, "");
    }

    static void replaceAll(
        std::string& str, const std::string& search, const std::string& replacement);

    static std::string extractStringBetween(const std::string& input, char delimiterStart,
        char delimiterEnd, bool includeDelimiter = false);

    static std::string removeEscapedCharacters(const std::string& input);
};

} // namespace common
} // namespace kuzu
