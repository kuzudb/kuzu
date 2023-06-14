#include "test_runner/test_parser.h"

#include <fstream>

#include "common/string_utils.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

std::unique_ptr<TestGroup> TestParser::parseTestFile() {
    openFile();
    parseHeader();
    if (!testGroup->isValid()) {
        throw TestException("Invalid test header [" + path + "].");
    }
    parseBody();
    return std::move(testGroup);
}

void TestParser::extractDataset() {
    const std::string& params = paramsToString(2);
    const std::string& datasetType = currentToken.params[1];
    if (datasetType == "PARQUET") {
        if (params.starts_with("CSV_TO_PARQUET(") && params.back() == ')') {
            testGroup->datasetType = TestGroup::DatasetType::CSV_TO_PARQUET;
            testGroup->dataset = params.substr(15, params.length() - 16);
        } else {
            testGroup->datasetType = TestGroup::DatasetType::PARQUET;
            testGroup->dataset = currentToken.params[2];
        }
    } else if (datasetType == "CSV") {
        testGroup->datasetType = TestGroup::DatasetType::CSV;
        testGroup->dataset = currentToken.params[2];
    } else if (datasetType == "NPY") {
        testGroup->datasetType = TestGroup::DatasetType::NPY;
        testGroup->dataset = currentToken.params[2];
    } else {
        throw TestException(
            "Invalid dataset type `" + currentToken.params[1] + "` [" + path + ":" + line + "].");
    }
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
            checkMinimumParams(2);
            extractDataset();
            break;
        }
        case TokenType::BUFFER_POOL_SIZE: {
            checkMinimumParams(1);
            testGroup->bufferPoolSize = stoi(currentToken.params[1]);
            break;
        }
        case TokenType::SKIP: {
            testGroup->group = "DISABLED_" + testGroup->group;
            break;
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
        nextLine();
        if (line.starts_with("<FILE>:")) {
            statement->expectedTuplesCSVFile = TestHelper::appendKuzuRootPath(
                FileUtils::joinPath(TestHelper::TEST_ANSWERS_PATH, line.substr(7)));
            return;
        }
        setCursorToPreviousLine();
        for (auto i = 0u; i < statement->expectedNumTuples; i++) {
            nextLine();
            replaceVariables(line);
            statement->expectedTuples.push_back(line);
        }
        if (!statement->checkOutputOrder) { // order is not important for result
            sort(statement->expectedTuples.begin(), statement->expectedTuples.end());
        }
    }
}

std::string TestParser::extractTextBeforeNextStatement(bool ignoreLineBreak) {
    std::string extractedText;
    const std::string delimiter = ignoreLineBreak ? " " : "\n";
    while (nextLine()) {
        tokenize();
        if (currentToken.type != TokenType::_SKIP_LINE) {
            setCursorToPreviousLine();
            break;
        }
        if (!extractedText.empty()) {
            extractedText += delimiter;
        }
        extractedText += line;
    }
    return extractedText;
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
        query += extractTextBeforeNextStatement(true);
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
    // REPEAT 3 "Alice " = "Alice Alice Alice "
    if (currentToken.params[2] == "REPEAT") {
        checkMinimumParams(4);
        return parseCommandRepeat();
    }
    // ARANGE 0 3 = [0,1,2,3]
    if (currentToken.params[2] == "ARANGE") {
        checkMinimumParams(4);
        return parseCommandArange();
    }
    auto params = paramsToString(2);
    if (params.front() != '"' || params.back() != '"') {
        throw TestException("Invalid DEFINE data type [" + path + ":" + line + "].");
    }
    return params.substr(1, params.size() - 2);
}

std::string TestParser::parseCommandRepeat() {
    int times = stoi(currentToken.params[3]);
    std::string result;
    std::string repeatString = StringUtils::extractStringBetween(paramsToString(4), '"', '"');
    if (repeatString.empty()) {
        throw TestException("Invalid DEFINE data type [" + path + ":" + line + "].");
    }
    for (auto i = 0; i < times; i++) {
        result += repeatString;
    }
    return result;
}
std::string TestParser::parseCommandArange() {
    int start = stoi(currentToken.params[3]);
    int end = stoi(currentToken.params[4]);
    std::string result = "[";
    for (auto i = start; i <= end; i++) {
        result += std::to_string(i);
        if (i != end) {
            result += ",";
        }
    }
    result += "]";
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
        case TokenType::SKIP: {
            testCaseName = "DISABLED_" + testCaseName;
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
        return TokenType::_SKIP_LINE;
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

} // namespace testing
} // namespace kuzu
