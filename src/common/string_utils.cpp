#include "common/string_utils.h"

#include <vector>

namespace kuzu {
namespace common {

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

} // namespace common
} // namespace kuzu
