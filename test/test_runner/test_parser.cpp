#include "test_runner/test_parser.h"

#include <fstream>
#include <numeric>

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

std::unique_ptr<TestGroup> TestParser::parseTestFile(const std::string& path) {
    openFile(path);
    parseHeader();
    if (testGroup->skipTest) {
        return std::move(testGroup);
    }
    if (!testGroup->isValid()) {
        throw Exception("Invalid test header");
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
            throw Exception("Invalid statement in test header");
        }
        }
    }
}

void TestParser::replaceVariables(std::string& str) {
    for (auto& variable : variableMap) {
        StringUtils::replaceAll(str, variable.first, variable.second);
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
        checkMinimumParams(1);
        std::string query = paramsToString();
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
        statement->encodedJoin = paramsToString();
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
        throw Exception("Invalid statement [" + line + "] in test file");
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
        throw Exception("Statement block not found [" + blockName + "]");
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
    currentToken.params = StringUtils::splitBySpace(line);
    if ((currentToken.params.size() == 0) || (currentToken.params[0][0] == '#')) {
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
