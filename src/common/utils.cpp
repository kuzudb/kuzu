#include "src/common/include/utils.h"

#include <string>
#include <vector>

namespace graphflow {
namespace common {

vector<string> StringUtils::split(const string& input, const string& delimiter) {
    auto result = vector<string>();
    auto prevPos = 0u;
    auto currentPos = input.find(delimiter, prevPos);
    while (currentPos != string::npos) {
        result.push_back(input.substr(prevPos, currentPos - prevPos));
        prevPos = currentPos + 1;
        currentPos = input.find(delimiter, prevPos);
    }
    result.push_back(input.substr(prevPos));
    return result;
}

string ThreadUtils::getThreadIDString() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

} // namespace common
} // namespace graphflow
