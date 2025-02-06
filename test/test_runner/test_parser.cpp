#include "test_runner/test_parser.h"

#include "test_runner/test_group.h"

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#ifdef __linux__
#include <features.h>
#ifndef __GLIBC__
#define __MUSL__
#endif
#endif

#include <filesystem>

#include "common/string_utils.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

std::unique_ptr<TestGroup> TestParser::parseTestFile() {
    openFile();
    genGroupName();
    parseHeader();
    if (!testGroup->isValid()) {
        throw TestException("Invalid test header [" + path + "].");
    }
    parseBody();
    return std::move(testGroup);
}

void TestParser::genGroupName() const {
    const std::size_t subStart =
        TestHelper::appendKuzuRootPath(std::string(TestHelper::E2E_TEST_FILES_DIRECTORY)).length() +
        1;
    const std::size_t subEnd = path.find_last_of('.') - 1;
    std::string relPath = path.substr(subStart, subEnd - subStart + 1);
    std::replace(relPath.begin(), relPath.end(), '/', '~');
    std::replace(relPath.begin(), relPath.end(), '\\', '~');
    testGroup->group = relPath;
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
    } else if (datasetType == "TTL") {
        testGroup->datasetType = TestGroup::DatasetType::TURTLE;
        testGroup->dataset = currentToken.params[2];
    } else if (datasetType == "KUZU") {
        testGroup->datasetType = TestGroup::DatasetType::KUZU;
        testGroup->dataset = currentToken.params[2];
    } else if (datasetType == "JSON") {
        if (params.starts_with("CSV_TO_JSON(") && params.back() == ')') {
            testGroup->datasetType = TestGroup::DatasetType::CSV_TO_JSON;
            testGroup->dataset = params.substr(12, params.length() - 13);
        } else {
            testGroup->datasetType = TestGroup::DatasetType::JSON;
            testGroup->dataset = currentToken.params[2];
        }
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
        case TokenType::DATASET: {
            checkMinimumParams(2);
            extractDataset();
            break;
        }
        case TokenType::BUFFER_POOL_SIZE: {
            checkMinimumParams(1);
            testGroup->bufferPoolSize = stoll(currentToken.params[1]);
            break;
        }
        case TokenType::SKIP: {
            testGroup->group = "DISABLED_" + testGroup->group;
            break;
        }
        case TokenType::SKIP_MUSL: {
#ifdef __MUSL__
            testGroup->group = "DISABLED_" + testGroup->group;
#endif
            break;
        }
        case TokenType::SKIP_32BIT: {
#ifdef __32BIT__
            testGroup->group = "DISABLED_" + testGroup->group;
#endif
            break;
        }
        case TokenType::SKIP_WASM: {
#ifdef __WASM__
            testGroup->group = "DISABLED_" + testGroup->group;
#endif
            break;
        }
        case TokenType::TEST_FWD_ONLY_REL: {
            testGroup->testFwdOnly = true;
            break;
        }
        case TokenType::SKIP_IN_MEM: {
            auto env = TestHelper::getSystemEnv("IN_MEM_MODE");
            if (!env.empty()) {
                if (env == "true") {
                    testGroup->group = "DISABLED_" + testGroup->group;
                }
            }
            break;
        }
        case TokenType::SKIP_VECTOR_CAPACITY_TESTS: {
            if constexpr (VECTOR_CAPACITY_LOG_2 != DEFAULT_VECTOR_CAPACITY_LOG_2) {
                testGroup->group = "DISABLED_" + testGroup->group;
            }
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

void TestParser::replaceVariables(std::string& str) const {
    for (auto& variable : variableMap) {
        StringUtils::replaceAll(str, "${" + variable.first + "}", variable.second);
    }
}

void TestParser::extractExpectedResults(TestStatement* statement) {
    do {
        tokenize();
        if (currentToken.type == TokenType::EMPTY) {
            continue;
        }
        if (currentToken.type != TokenType::RESULT) {
            setCursorToPreviousLine();
            return;
        }
        statement->result.push_back(extractExpectedResultFromToken(statement->checkOutputOrder));
    } while (nextLine());
}

TestQueryResult TestParser::extractExpectedResultFromToken(bool checkOutputOrder) {
    checkMinimumParams(1);
    const std::string result = currentToken.params[1];
    TestQueryResult queryResult;
    if (result == "ok") {
        queryResult.type = ResultType::OK;
    } else if (result == "error") {
        queryResult.type = ResultType::ERROR_MSG;
        queryResult.expectedResult.push_back(extractTextBeforeNextStatement());
        replaceVariables(queryResult.expectedResult[0]);
    } else if (result == "error(regex)") {
        queryResult.type = ResultType::ERROR_REGEX;
        queryResult.expectedResult.push_back(extractTextBeforeNextStatement());
        replaceVariables(queryResult.expectedResult[0]);
    } else if (result.substr(0, 4) == "hash") {
        queryResult.type = ResultType::HASH;
        checkMinimumParams(1);
        nextLine();
        tokenize();
        queryResult.numTuples = stoi(currentToken.params[0]);
        queryResult.expectedResult.push_back(currentToken.params.back());
    } else {
        checkMinimumParams(1);
        queryResult.numTuples = stoi(result);
        nextLine();
        if (line.starts_with("<FILE>:")) {
            queryResult.type = ResultType::CSV_FILE;
            queryResult.expectedResult.push_back(TestHelper::appendKuzuRootPath(
                (std::filesystem::path(TestHelper::TEST_ANSWERS_PATH) / line.substr(7)).string()));
        } else {
            queryResult.type = ResultType::TUPLES;
            setCursorToPreviousLine();
            for (auto i = 0u; i < queryResult.numTuples; i++) {
                nextLine();
                replaceVariables(line);
                queryResult.expectedResult.push_back(line);
            }
            if (!checkOutputOrder) { // order is not important for result
                sort(queryResult.expectedResult.begin(), queryResult.expectedResult.end());
            }
        }
    }
    return queryResult;
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

TestStatement* TestParser::extractStatement(TestStatement* statement,
    const std::string& testCaseName) {
    tokenize();
    switch (currentToken.type) {
    case TokenType::LOG: {
        checkMinimumParams(1);
        statement->logMessage = paramsToString(1);
        break;
    }
    case TokenType::CHECKPOINT_WAIT_TIMEOUT: {
        checkMinimumParams(1);
        testGroup->checkpointWaitTimeout = stoi(currentToken.params[1]);
        break;
    }
    case TokenType::CREATE_CONNECTION: {
        checkMinimumParams(1);
        testGroup->testCasesConnNames[testCaseName].insert(currentToken.params[1]);
        break;
    }
    case TokenType::BEGIN_CONCURRENT_EXECUTION: {
        statement->connectionsStatusFlag = ConcurrentStatusFlag::BEGIN;
        return statement;
    }
    case TokenType::END_CONCURRENT_EXECUTION: {
        statement->connectionsStatusFlag = ConcurrentStatusFlag::END;
        return statement;
    }
    case TokenType::RELOADDB: {
        statement->reloadDBFlag = true;
        return statement;
    }
    case TokenType::CREATE_DATASET_SCHEMA: {
        statement->dataset = getParam(1);
        statement->manualUseDataset = ManualUseDatasetFlag::SCHEMA;
        return statement;
    }
    case TokenType::INSERT_DATASET_BY_ROW: {
        statement->dataset = getParam(1);
        statement->manualUseDataset = ManualUseDatasetFlag::INSERT;
        return statement;
    }
    case TokenType::IMPORT_DATABASE: {
        statement->importDBFlag = true;
        auto filePath = getParam(1);
        replaceVariables(filePath);
        statement->importFilePath = filePath;
        return statement;
    }
    case TokenType::REMOVE_FILE: {
        statement->removeFileFlag = true;
        auto filePath = getParam(1);
        replaceVariables(filePath);
        statement->removeFilePath = filePath;
        return statement;
    }
    case TokenType::MULTI_COPY_RANDOM: {
        statement->multiCopySplits = stoll(getParam(1));
        statement->multiCopyTable = getParam(2);
        int rest = 3;
        if (getParam(3) == "SEED") {
            statement->seed.resize(2);
            statement->seed[0] = stoll(getParam(4));
            statement->seed[1] = stoll(getParam(5));
            rest = 6;
        }
        auto multiCopySource = paramsToString(rest);
        replaceVariables(multiCopySource);
        statement->multiCopySource = multiCopySource;
        return statement;
    }
    case TokenType::STATEMENT: {
        std::string query = paramsToString(1);
        extractConnName(query, statement);
        query += extractTextBeforeNextStatement(true);
        replaceVariables(query);
        statement->query = query;
        break;
    }
    case TokenType::SET: {
        auto envName = getParam(1);
        auto envValue = getParam(2);
#if defined(_WIN32)
        _putenv_s(envName.c_str(), envValue.c_str());
#else
        // NOLINTNEXTLINE(*-mt-unsafe)
        setenv(envName.c_str(), envValue.c_str(), 1 /* overwrite existing env*/);
#endif
        break;
    }
    case TokenType::BATCH_STATEMENTS: {
        std::string query = paramsToString(1);
        extractConnName(query, statement);
        statement->batchStatmentsCSVFile = TestHelper::appendKuzuRootPath(
            (std::filesystem::path(TestHelper::TEST_STATEMENTS_PATH) / query.substr(7)).string());
        break;
    }
    case TokenType::RESULT: {
        extractExpectedResults(statement);
        return statement;
    }
    case TokenType::CHECK_ORDER: {
        statement->checkOutputOrder = true;
        break;
    }
    case TokenType::CHECK_PRECISION: {
        statement->checkPrecision = true;
        break;
    }
    case TokenType::PARALLELISM: {
        checkMinimumParams(1);
        statement->numThreads = stoi(currentToken.params[1]);
        break;
    }
    case TokenType::CHECK_COLUMN_NAMES: {
        statement->checkColumnNames = true;
        break;
    }
    case TokenType::EMPTY: {
        break;
    }
    default: {
        throw TestException("Invalid statement [" + path + ":" + line + "].");
    }
    }
    nextLine();
    return extractStatement(statement, testCaseName);
}

void TestParser::extractStatementBlock() {
    const std::string blockName = currentToken.params[1];
    while (nextLine()) {
        tokenize();
        if (currentToken.type == TokenType::END_OF_STATEMENT_BLOCK) {
            break;
        } else {
            auto statement = std::make_unique<TestStatement>();
            extractStatement(statement.get(), blockName);
            testGroup->testCasesStatementBlocks[blockName].push_back(std::move(statement));
            testGroup->testCasesConnNames[blockName] = std::set<std::string>();
        }
    }
}

std::string TestParser::parseCommand() {
    // REPEAT 3 "col${count}, " = "col0, col1, col2, "
    if (currentToken.params[2] == "REPEAT") {
        checkMinimumParams(4);
        return parseCommandRepeat();
    }
    // ARANGE 0 3 = [0,1,2,3]
    if (currentToken.params[2] == "ARANGE") {
        checkMinimumParams(4);
        return parseCommandArange();
    }
    const auto params = paramsToString(2);
    if (params.front() != '"' || params.back() != '"') {
        throw TestException("Invalid DEFINE data type [" + path + ":" + line + "].");
    }
    return params.substr(1, params.size() - 2);
}

std::string TestParser::parseCommandRepeat() {
    const int times = stoi(currentToken.params[3]);
    std::string result;
    const std::string repeatString = StringUtils::extractStringBetween(paramsToString(4), '"', '"');
    if (repeatString.empty()) {
        throw TestException("Invalid DEFINE data type [" + path + ":" + line + "].");
    }
    for (auto i = 1; i <= times; i++) {
        auto stringToAppend = repeatString;
        StringUtils::replaceAll(stringToAppend, "${count}", std::to_string(i));
        result += stringToAppend;
    }
    return result;
}

std::string TestParser::parseCommandArange() const {
    const int start = stoi(currentToken.params[3]);
    const int end = stoi(currentToken.params[4]);
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
    bool testFwdOnly = testGroup->testFwdOnly;
    std::string testCaseName;
    while (nextLine()) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::CASE: {
            checkMinimumParams(1);
            testCaseName = currentToken.params[1];
            testFwdOnly = testGroup->testFwdOnly;
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
        case TokenType::INSERT_STATEMENT_BLOCK: {
            checkMinimumParams(1);
            addStatementBlock(currentToken.params[1], testCaseName);
            break;
        }
        case TokenType::SKIP: {
            testCaseName = "DISABLED_" + testCaseName;
            break;
        }
        case TokenType::SKIP_MUSL: {
#ifdef __MUSL__
            testCaseName = "DISABLED_" + testCaseName;
#endif
            break;
        }
        case TokenType::SKIP_32BIT: {
#ifdef __32BIT__
            testCaseName = "DISABLED_" + testCaseName;
#endif
            break;
        }
        case TokenType::SKIP_WASM: {
#ifdef __WASM__
            testCaseName = "DISABLED_" + testCaseName;
#endif
            break;
        }
        case TokenType::SKIP_VECTOR_CAPACITY_TESTS: {
            if constexpr (VECTOR_CAPACITY_LOG_2 != DEFAULT_VECTOR_CAPACITY_LOG_2) {
                testCaseName = "DISABLED_" + testCaseName;
            }
            break;
        }
        case TokenType::SKIP_IN_MEM: {
            auto env = TestHelper::getSystemEnv("IN_MEM_MODE");
            if (!env.empty()) {
                if (env == "true") {
                    testCaseName = "DISABLED_" + testCaseName;
                }
            }
            break;
        }
        case TokenType::TEST_FWD_ONLY_REL: {
            testFwdOnly = true;
            break;
        }
        case TokenType::EMPTY: {
            break;
        }
        default: {
            if (TestHelper::getSystemEnv("DEFAULT_REL_STORAGE_DIRECTION") == "fwd" &&
                !testFwdOnly && !testCaseName.starts_with("DISABLED_")) {
                testCaseName = "DISABLED_" + testCaseName;
            }
            // if its not a special case, then it has to be a statement
            TestStatement* statement = addNewStatement(testCaseName);
            testGroup->testCasesConnNames[testCaseName].insert(TestHelper::DEFAULT_CONN_NAME);
            extractStatement(statement, testCaseName);
        }
        }
    }
}

void TestParser::addStatementBlock(const std::string& blockName,
    const std::string& testCaseName) const {
    if (testGroup->testCasesStatementBlocks.contains(blockName)) {
        for (const auto& statementPtr : testGroup->testCasesStatementBlocks[blockName]) {
            testGroup->testCases[testCaseName].push_back(
                std::make_unique<TestStatement>(*statementPtr));
            testGroup->testCasesConnNames[testCaseName] = testGroup->testCasesConnNames[blockName];
        }
    } else {
        throw TestException("Statement block not found [" + path + ":" + blockName + "].");
    }
}

TestStatement* TestParser::addNewStatement(const std::string& testGroupName) const {
    auto statement = std::make_unique<TestStatement>();
    TestStatement* currentStatement = statement.get();
    testGroup->testCases[testGroupName].push_back(std::move(statement));
    return currentStatement;
}

TokenType getTokenType(const std::string& input) {
    const auto iter = tokenMap.find(input);
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

static std::vector<std::string> extractToken(std::string& line) {
    std::vector<std::string> matches;
    std::regex re(R"((?:[^'"\s\\]+|'[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*"|\S+)+)");
    auto wordsBegin = std::sregex_iterator(line.begin(), line.end(), re);
    auto wordsEnd = std::sregex_iterator();
    for (std::sregex_iterator i = wordsBegin; i != wordsEnd; ++i) {
        std::smatch match = *i;
        matches.push_back(match.str());
    }
    return matches;
}

void TestParser::tokenize() {
    currentToken.params = extractToken(line);
    if ((currentToken.params.size() == 0) || (currentToken.params[0][0] == '#')) {
        currentToken.type = TokenType::EMPTY;
    } else {
        currentToken.type = getTokenType(currentToken.params[0]);
    }
}

void TestParser::extractConnName(std::string& query, TestStatement* statement) {
    const std::regex pattern(R"(\[(conn.*?)\]\s*(.*))");
    std::smatch matches;
    bool statement_status = false;
    if (std::regex_search(query, matches, pattern)) {
        if (matches.size() == 3) {
            statement_status = true;
            statement->connName = matches[1];
            query = matches[2];
        }
    }
    if (!statement_status) {
        statement->connName = TestHelper::DEFAULT_CONN_NAME;
    }
}

} // namespace testing
} // namespace kuzu
