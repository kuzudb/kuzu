#include "test_helper/test_parser.h"

#include <fstream>
#include <numeric>

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

TokenType getTokenType(const std::string& input) {
    auto iter = tokenMap.find(input);
    if (iter != tokenMap.end()) {
        return iter->second;
    } else {
        return TokenType::SKIP;
    }
}

void TestParser::openFile(const std::string& path) {
    if (access(path.c_str(), 0) != 0) {
        throw Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw Exception("Test file is a directory. [" + path + "].");
    }
    fileStream.open(path);
}

void TestParser::tokenize() {
    currentToken.params = StringUtils::splitByAnySpace(line);
    if (currentToken.params.size() == 0) {
        currentToken.type = TokenType::EMPTY;
    } else {
        currentToken.type = getTokenType(currentToken.params[0]);
    }
}

std::string TestParser::paramsToString() {
    return std::accumulate(std::next(currentToken.params.begin()), currentToken.params.end(),
        std::string(),
        [](const std::string& a, const std::string& b) { return a + (a.empty() ? "" : " ") + b; });
}

} // namespace testing
} // namespace kuzu
