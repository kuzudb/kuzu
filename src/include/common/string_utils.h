#pragma once

#include <regex>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

class StringUtils {
public:
    static std::vector<std::string> splitComma(const std::string& input);

    static std::vector<std::string> split(
        const std::string& input, const std::string& delimiter, bool ignoreEmptyStringParts = true);

    static std::vector<std::string> splitBySpace(const std::string& input);

    static void toUpper(std::string& input) {
        std::transform(input.begin(), input.end(), input.begin(), ::toupper);
    }
    static std::string getUpper(const std::string& input) {
        auto result = input;
        toUpper(result);
        return result;
    }

    static void toLower(std::string& input) {
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
    }

    static bool CharacterIsSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
    }

    static bool CharacterIsDigit(char c) { return c >= '0' && c <= '9'; }

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

    static void removeCStringWhiteSpaces(const char*& input, uint64_t& len);

    static void replaceAll(
        std::string& str, const std::string& search, const std::string& replacement);

    static std::string extractStringBetween(const std::string& input, char delimiterStart,
        char delimiterEnd, bool includeDelimiter = false);

    static std::string removeEscapedCharacters(const std::string& input);
};

} // namespace common
} // namespace kuzu
