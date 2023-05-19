#include "test_helper/test_parser.h"

#include <fstream>
#include <numeric>

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

std::unique_ptr<TestCase> TestParser::parseTestFile(const std::string& path) {
    openFile(path);
    parseHeader();
    if (testCase->skipTest) {
        return std::move(testCase);
    }
    if (!testCase->isValid()) {
        throw Exception("Invalid test header");
    }
    parseBody();
    return std::move(testCase);
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
            testCase->group = currentToken.params[1];
            break;
        }
        case TokenType::TEST: {
            checkMinimumParams(1);
            testCase->name = currentToken.params[1];
            break;
        }
        case TokenType::DATASET: {
            checkMinimumParams(1);
            testCase->dataset = currentToken.params[1];
            break;
        }
        case TokenType::SKIP: {
            testCase->skipTest = true;
            return;
        }
        case TokenType::SEPARATOR: {
            return;
            break;
        }
        default: {
            throw Exception("Invalid statement in test header");
        }
        }
    }
}

void TestParser::extractExpectedResult(TestStatement* statement) {
    checkMinimumParams(1);
    std::string result = currentToken.params[1];
    if (result == "ok") {
        statement->expectedOk = true;
    } else if (result == "error") {
        statement->expectedError = true;
        nextLine();
        statement->errorMessage = StringUtils::ltrim(line);
    } else {
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

TestStatement* TestParser::extractStatement(TestStatement* statement) {
    if (endOfFile()) {
        return statement;
    }
    tokenize();
    switch (currentToken.type) {
    case TokenType::STATEMENT:
    case TokenType::QUERY: {
        statement->query = paramsToString();
        break;
    }
    case TokenType::RESULT: {
        extractExpectedResult(statement);
        return statement;
        break;
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
            TestStatement* statement = addNewStatement(testCase->variableStatements);
            statement->blockName = blockName;
            extractStatement(statement);
        }
    }
}

void TestParser::parseBody() {
    while (nextLine()) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::CASE:
        case TokenType::NAME: {
            checkMinimumParams(1);
            name = currentToken.params[1];
            break;
        }
        case TokenType::DEFINE_STATEMENT_BLOCK: {
            checkMinimumParams(2);
            extractStatementBlock();
            break;
        }
        case TokenType::STATEMENT_BLOCK: {
            checkMinimumParams(1);
            for (auto& statement : testCase->variableStatements) {
                if (statement->blockName == currentToken.params[1]) {
                    statement->name = name;
                    testCase->statements.push_back(std::make_unique<TestStatement>(*statement));
                }
            }
            break;
        }
        case TokenType::EMPTY: {
            break;
        }
        default: {
            // if its not special case, then it has to be a statement
            TestStatement* statement = addNewStatement(testCase->statements);
            statement->name = name;
            extractStatement(statement);
        }
        }
    }
}

TestStatement* TestParser::addNewStatement(
    std::vector<std::unique_ptr<TestStatement>>& statementsVector) {
    TestStatement* currentStatement;
    auto statement = std::make_unique<TestStatement>();
    currentStatement = statement.get();
    statementsVector.push_back(std::move(statement));
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
    currentToken.params = StringUtils::splitByAnySpace(line);
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
