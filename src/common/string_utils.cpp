#include "common/string_utils.h"

#include <vector>

namespace kuzu {
namespace common {

std::vector<std::string> StringUtils::split(
    const std::string& input, const std::string& delimiter) {
    auto result = std::vector<std::string>();
    auto prevPos = 0u;
    auto currentPos = input.find(delimiter, prevPos);
    while (currentPos != std::string::npos) {
        result.push_back(input.substr(prevPos, currentPos - prevPos));
        prevPos = currentPos + 1;
        currentPos = input.find(delimiter, prevPos);
    }
    result.push_back(input.substr(prevPos));
    return result;
}

} // namespace common
} // namespace kuzu
