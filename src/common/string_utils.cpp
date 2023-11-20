#include "common/string_utils.h"

#include <sstream>
#include <vector>

namespace kuzu {
namespace common {

std::vector<std::string> StringUtils::splitComma(const std::string& input) {
    auto result = std::vector<std::string>();
    auto currentPos = 0u;
    auto lvl = 0u;
    while (currentPos < input.length()) {
        if (input[currentPos] == '(') {
            lvl++;
        } else if (input[currentPos] == ')') {
            lvl--;
        } else if (lvl == 0 && input[currentPos] == ',') {
            break;
        }
        currentPos++;
    }
    result.push_back(input.substr(0, currentPos));
    result.push_back(input.substr(currentPos == input.length() ? input.length() : currentPos + 1));
    return result;
}

std::vector<std::string> StringUtils::split(
    const std::string& input, const std::string& delimiter, bool ignoreEmptyStringParts) {
    auto result = std::vector<std::string>();
    auto prevPos = 0u;
    auto currentPos = input.find(delimiter, prevPos);
    while (currentPos != std::string::npos) {
        auto stringPart = input.substr(prevPos, currentPos - prevPos);
        if (!ignoreEmptyStringParts || !stringPart.empty()) {
            result.push_back(input.substr(prevPos, currentPos - prevPos));
        }
        prevPos = currentPos + 1;
        currentPos = input.find(delimiter, prevPos);
    }
    result.push_back(input.substr(prevPos));
    return result;
}

std::vector<std::string> StringUtils::splitBySpace(const std::string& input) {
    std::istringstream iss(input);
    std::vector<std::string> result;
    std::string token;
    while (iss >> token)
        result.push_back(token);
    return result;
}

void StringUtils::removeCStringWhiteSpaces(const char*& input, uint64_t& len) {
    // skip leading/trailing spaces
    while (len > 0 && isspace(input[0])) {
        input++;
        len--;
    }
    while (len > 0 && isspace(input[len - 1])) {
        len--;
    }
}

void StringUtils::replaceAll(
    std::string& str, const std::string& search, const std::string& replacement) {
    size_t pos = 0;
    while ((pos = str.find(search, pos)) != std::string::npos) {
        str.replace(pos, search.length(), replacement);
        pos += replacement.length();
    }
}

std::string StringUtils::extractStringBetween(
    const std::string& input, char delimiterStart, char delimiterEnd, bool includeDelimiter) {
    std::string::size_type posStart = input.find_first_of(delimiterStart);
    std::string::size_type posEnd = input.find_last_of(delimiterEnd);
    if (posStart == std::string::npos || posEnd == std::string::npos || posStart >= posEnd) {
        return "";
    }
    if (includeDelimiter) {
        posEnd++;
    } else {
        posStart++;
    }
    return input.substr(posStart, posEnd - posStart);
}

std::string StringUtils::removeEscapedCharacters(const std::string& input) {
    std::string resultStr;
    for (auto i = 1u; i < input.length() - 1; i++) {
        // Antlr4 already guarantees that the character followed by the escaped character is
        // valid. So we can safely skip the escaped character.
        if (input[i] == '\\') {
            i++;
        }
        resultStr += input[i];
    }
    return resultStr;
}

} // namespace common
} // namespace kuzu
