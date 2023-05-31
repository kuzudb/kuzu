#include "test_runner/test_parser.h"

#include <fstream>
#include <numeric>

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

std::unique_ptr<TestGroup> TestParser::parseTestFile() {
    openFile();
    parseHeader();
    if (testGroup->skipTest) {
        return std::move(testGroup);
    }
    if (!testGroup->isValid()) {
        throw TestException("Invalid test header [" + path + "].");
    }
    parseBody();
    return std::move(testGroup);
}

void TestParser::parseHeader() {
    while (nextLine()) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::EMPTY: {
            break;
        }
        case TokenType::GROUP: {
            checkMinimumParams(1);
            testGroup->group = currentToken.params[1];
            break;
        }
        case TokenType::DATASET: {
            checkMinimumParams(1);
            testGroup->dataset = currentToken.params[1];
            break;
        }
        case TokenType::BUFFER_POOL_SIZE: {
            checkMinimumParams(1);
            testGroup->bufferPoolSize = stoi(currentToken.params[1]);
            break;
        }
        case TokenType::SKIP: {
            testGroup->skipTest = true;
            return;
        }
        case TokenType::SEPARATOR: {
            return;
        }
        default: {
            throw TestException("Invalid test header statement [" + path + ":" + line + "].");
        }
        }
    }
}

void TestParser::replaceVariables(std::string& str) {
    for (auto& variable : variableMap) {
        StringUtils::replaceAll(str, "${" + variable.first + "}", variable.second);
    }
}

void TestParser::extractExpectedResult(TestStatement* statement) {
    checkMinimumParams(1);
    std::string result = currentToken.params[1];
    if (result == "ok") {
        statement->expectedOk = true;
    } else if (result == "error") {
        statement->expectedError = true;
        statement->errorMessage = extractTextBeforeNextStatement();
        replaceVariables(statement->errorMessage);
    } else {
        checkMinimumParams(1);
        statement->expectedNumTuples = stoi(result);
        for (auto i = 0u; i < statement->expectedNumTuples; i++) {
            nextLine();
            statement->expectedTuples.push_back(line);
        }
        if (!statement->checkOutputOrder) { // order is not important for result
            sort(statement->expectedTuples.begin(), statement->expectedTuples.end());
        }
    }
}

std::string TestParser::extractTextBeforeNextStatement() {
    std::string text;
    while (nextLine()) {
        tokenize();
        if (currentToken.type != TokenType::SKIP) {
            setCursorToPreviousLine();
            break;
        }
        if (!text.empty()) {
            text += "\n";
        }
        text += StringUtils::ltrim(line);
    }
    return text;
}

TestStatement* TestParser::extractStatement(TestStatement* statement) {
    if (endOfFile()) {
        return statement;
    }
    tokenize();
    switch (currentToken.type) {
    case TokenType::NAME: {
        checkMinimumParams(1);
        statement->name = currentToken.params[1];
        break;
    }
    case TokenType::STATEMENT:
    case TokenType::QUERY: {
        std::string query = paramsToString(1);
        replaceVariables(query);
        statement->query = query;
        break;
    }
    case TokenType::RESULT: {
        extractExpectedResult(statement);
        return statement;
    }
    case TokenType::CHECK_ORDER: {
        statement->checkOutputOrder = true;
        break;
    }
    case TokenType::ENUMERATE: {
        statement->enumerate = true;
        break;
    }
    case TokenType::PARALLELISM: {
        checkMinimumParams(1);
        statement->numThreads = stoi(currentToken.params[1]);
        break;
    }
    case TokenType::ENCODED_JOIN: {
        statement->encodedJoin = paramsToString(1);
        break;
    }
    case TokenType::BEGIN_WRITE_TRANSACTION: {
        statement->isBeginWriteTransaction = true;
        return statement;
    }
    case TokenType::EMPTY: {
        break;
    }
    default: {
        throw TestException("Invalid statement [" + path + ":" + line + "].");
    }
    }
    nextLine();
    return extractStatement(statement);
}

void TestParser::extractStatementBlock() {
    std::string blockName = currentToken.params[1];
    while (nextLine()) {
        tokenize();
        if (currentToken.type == TokenType::END_OF_STATEMENT_BLOCK) {
            break;
        } else {
            auto statement = std::make_unique<TestStatement>();
            extractStatement(statement.get());
            testGroup->testCasesStatementBlocks[blockName].push_back(std::move(statement));
        }
    }
}

std::string TestParser::parseCommand() {
    std::string result;
    if (currentToken.params[2] == "ARANGE") {
        checkMinimumParams(4);
        int start = stoi(currentToken.params[3]);
        int end = stoi(currentToken.params[4]);
        result = "[";
        for (auto i = start; i <= end; i++) {
            result += std::to_string(i);
            if (i != end) {
                result += ",";
            }
        }
        result += "]";
    } else {
        result = paramsToString(2);
    }
    return result;
}

void TestParser::parseBody() {
    std::string testCaseName;
    while (nextLine()) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::CASE: {
            checkMinimumParams(1);
            testCaseName = currentToken.params[1];
            break;
        }
        case TokenType::DEFINE_STATEMENT_BLOCK: {
            checkMinimumParams(2);
            extractStatementBlock();
            break;
        }
        case TokenType::DEFINE: {
            checkMinimumParams(2);
            variableMap[currentToken.params[1]] = parseCommand();
            break;
        }

        case TokenType::STATEMENT_BLOCK: {
            checkMinimumParams(1);
            addStatementBlock(currentToken.params[1], testCaseName);
            break;
        }
        case TokenType::EMPTY: {
            break;
        }
        default: {
            // if its not a special case, then it has to be a statement
            TestStatement* statement = addNewStatement(testCaseName);
            extractStatement(statement);
        }
        }
    }
}

void TestParser::addStatementBlock(const std::string& blockName, const std::string& testCaseName) {
    if (testGroup->testCasesStatementBlocks.find(blockName) !=
        testGroup->testCasesStatementBlocks.end()) {
        for (const auto& statementPtr : testGroup->testCasesStatementBlocks[blockName]) {
            testGroup->testCases[testCaseName].push_back(
                std::make_unique<TestStatement>(*statementPtr));
        }
    } else {
        throw TestException("Statement block not found [" + path + ":" + blockName + "].");
    }
}

TestStatement* TestParser::addNewStatement(std::string& testGroupName) {
    auto statement = std::make_unique<TestStatement>();
    TestStatement* currentStatement = statement.get();
    testGroup->testCases[testGroupName].push_back(std::move(statement));
    return currentStatement;
}

TokenType getTokenType(const std::string& input) {
    auto iter = tokenMap.find(input);
    if (iter != tokenMap.end()) {
        return iter->second;
    } else {
        return TokenType::SKIP;
    }
}

void TestParser::openFile() {
    if (access(path.c_str(), 0) != 0) {
        throw TestException("Test file not exists [" + path + "].");
    }
    fileStream.open(path);
}

void TestParser::tokenize() {
    currentToken.params = StringUtils::splitBySpace(line);
    if ((currentToken.params.size() == 0) || (currentToken.params[0][0] == '#')) {
        currentToken.type = TokenType::EMPTY;
    } else {
        currentToken.type = getTokenType(currentToken.params[0]);
    }
}

std::string TestParser::paramsToString(int startToken) {
    return std::accumulate(std::next(currentToken.params.begin(), startToken),
        currentToken.params.end(), std::string(),
        [](const std::string& a, const std::string& b) { return a + (a.empty() ? "" : " ") + b; });
}

} // namespace testing
} // namespace kuzu
