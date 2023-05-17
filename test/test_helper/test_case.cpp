#include "test_helper/test_case.h"

#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

void TestCase::parseTestFile(const std::string& path) {
    TestParser parser;
    parser.openFile(path);
    parseHeader(parser);
    if (!isHeaderValid()) {
        throw Exception("Invalid test header");
    }
    if (skipTest) {
        return;
    }
    parseBody(parser);
}

void TestCase::parseHeader(TestParser& parser) {
    while (parser.nextLine()) {
        parser.tokenize();
        switch (parser.currentToken.type) {
        case TokenType::EMPTY: {
            break;
        }
        case TokenType::GROUP: {
            checkMinimumParams(parser, 1);
            group = parser.getParam(1);
            break;
        }
        case TokenType::TEST: {
            checkMinimumParams(parser, 1);
            name = parser.getParam(1);
            break;
        }
        case TokenType::DATASET: {
            checkMinimumParams(parser, 1);
            dataset = parser.getParam(1);
            break;
        }
        case TokenType::SKIP: {
            skipTest = true;
            break;
        }
        case TokenType::SEPARATOR: { // end of header
            return;
            break;
        }
        default: {
            throw Exception("Invalid statement in test header");
        }
        }
    }
}

void TestCase::extractExpectedResult(TestParser& parser, TestStatement* currentStatement) {
    checkMinimumParams(parser, 1);
    std::string result = parser.getParam(1);
    if (result == "ok") {
        currentStatement->expectedOk = true;
    } else if (result == "error") {
        currentStatement->expectedError = true;
        parser.nextLine();
        currentStatement->errorMessage = StringUtils::ltrim(parser.line);
    } else {
        currentStatement->expectedNumTuples = stoi(result);
        for (auto i = 0u; i < currentStatement->expectedNumTuples; i++) {
            parser.nextLine();
            currentStatement->expectedTuples.push_back(parser.line);
        }
        if (!currentStatement->checkOutputOrder) { // order is not important for result
            sort(currentStatement->expectedTuples.begin(), currentStatement->expectedTuples.end());
        }
    }
}

TestStatement* TestCase::extractStatement(TestParser& parser, TestStatement* currentStatement) {
    if (parser.endOfFile()) {
        return currentStatement;
    }
    parser.tokenize();
    switch (parser.currentToken.type) {
    case TokenType::CASE:
    case TokenType::NAME: {
        currentStatement->name = parser.getParam(1);
        break;
    }
    case TokenType::STATEMENT:
    case TokenType::QUERY: {
        currentStatement->query = parser.paramsToString();
        break;
    }
    case TokenType::RESULT: {
        extractExpectedResult(parser, currentStatement);
        return currentStatement;
        break;
    }
    case TokenType::CHECK_ORDER: {
        currentStatement->checkOutputOrder = true;
        break;
    }
    case TokenType::ENUMERATE: {
        currentStatement->enumerate = true;
        break;
    }
    case TokenType::PARALLELISM: {
        checkMinimumParams(parser, 1);
        currentStatement->numThreads = stoi(parser.getParam(1));
        break;
    }
    case TokenType::EMPTY: {
        break;
    }
    default: {
        throw Exception("Invalid statement [" + parser.line + "] in test file");
    }
    }
    parser.nextLine();
    return extractStatement(parser, currentStatement);
}

void TestCase::extractStatementBlock(TestParser& parser) {
    std::string blockName = parser.getParam(1);
    while (parser.nextLine()) {
        parser.tokenize();
        if (parser.currentToken.type == TokenType::END_OF_STATEMENT_BLOCK) {
            break;
        } else {
            TestStatement* statement = addNewStatement(variableStatements);
            statement->blockName = blockName;
            extractStatement(parser, statement);
        }
    }
}

void TestCase::parseBody(TestParser& parser) {
    parser.nextLine();
    if (parser.endOfFile()) {
        return;
    }
    parser.tokenize();
    switch (parser.currentToken.type) {
    case TokenType::DEFINE_STATEMENT_BLOCK: {
        checkMinimumParams(parser, 2);
        extractStatementBlock(parser);
        break;
    }
    case TokenType::STATEMENT_BLOCK: {
        checkMinimumParams(parser, 1);
        for (auto& statement : variableStatements) {
            if (statement->blockName == parser.getParam(1)) {
                statements.push_back(std::move(statement));
            }
        }
        break;
    }
    case TokenType::LOOP: {
        //  FOR 0 100
        // for (i=0 to 100) {
        //   tokenReplacement = [$i, number]
        //   addNewStatement
        //   extractStatement(parser, statement, tokenReplacement)
        // }
        break;
    }
    case TokenType::FOREACH: {
        // -FOREACH cat_breeds siamese coon burmese persian
        // for (auto& items : parser.currentToken.params) {
        //   tokenReplacement = [$cat_breeds, coon]
        //   addNewStatement
        //   extractStatement(parser, statement, tokenReplacement)
        // }
        break;
    }
    case TokenType::EMPTY: {
        break;
    }
    default: {
        // if its not special case, then it has to be a statement
        TestStatement* currentStatement = addNewStatement(statements);
        extractStatement(parser, currentStatement);
    }
    }
    parseBody(parser);
}

// TODO: better naming?
TestStatement* TestCase::addNewStatement(
    std::vector<std::unique_ptr<TestStatement>>& statementsVector) {
    TestStatement* currentStatement;
    auto statement = std::make_unique<TestStatement>();
    currentStatement = statement.get();
    statementsVector.push_back(std::move(statement));
    return currentStatement;
}

void TestCase::checkMinimumParams(TestParser& parser, int minimumParams) {
    if (parser.currentToken.params.size() < minimumParams) {
        throw Exception("Invalid number of parameters for statement [" + parser.line + "]");
    }
}

} // namespace testing
} // namespace kuzu
