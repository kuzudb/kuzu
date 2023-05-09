#include "test_helper/test_parser.h"

#include <fstream>
#include <iostream> // REMOVE ME

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


void TestParser::openFile(const std::string &path) { 
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

// TODO: improve
void TestParser::tokenize() {
    if (line.empty()) {
        currentToken.type = TokenType::SKIP;
    }
    currentToken.params = splitString(line);
    if (currentToken.params.size() == 0) {
        currentToken.type = TokenType::SKIP;
    } else {
        currentToken.type = getTokenType(currentToken.params[0]);
    }
}

bool TestParser::nextLine() {
    return static_cast<bool>(getline(fileStream, line));
}

// TODO: move to test_utils
std::vector<std::string> TestParser::splitString(const std::string& input) {
    std::istringstream iss(input);
    std::vector<std::string> result;
    std::string token;
    while (iss >> token) result.push_back(token);
    return result;
}


} // namespace testing
} // namespace kuzu
