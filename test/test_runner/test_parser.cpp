#include "test_runner/test_parser.h"

#include <sys/stat.h>

#include "common/system_config.h"
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
#include <sstream>

#include "common/string_utils.h"
#include "common/types/timestamp_t.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

TestParser::TestParser(std::string path)
    : path(std::move(path)), testGroup(std::make_unique<TestGroup>()), currentToken() {
    variableMap.insert({"KUZU_ROOT_DIRECTORY", Value(KUZU_ROOT_DIRECTORY)});
    variableMap.insert({"KUZU_VERSION", Value(KUZU_VERSION)});
    variableMap.insert({"KUZU_EXPORT_DB_DIRECTORY", Value(exportDBPath)});
}

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
    std::ranges::replace(relPath, '/', '~');
    std::ranges::replace(relPath, '\\', '~');
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

bool TestParser::shouldSkip(TokenType type) {
    switch (type) {
    case TokenType::SKIP: {
        return true;
    }
    case TokenType::SKIP_MUSL: {
#ifdef __MUSL__
        return true;
#endif
    } break;
    case TokenType::SKIP_WASM: {
#ifdef __WASM__
        return true;
#endif
    } break;
    case TokenType::WASM_ONLY: {
#ifndef __WASM__
        return true;
#endif
    } break;
    case TokenType::SKIP_IN_MEM: {
        auto env = TestHelper::getSystemEnv("IN_MEM_MODE");
        if (!env.empty()) {
            if (env == "true") {
                return true;
            }
        }
    } break;
    case TokenType::SKIP_COMPRESSION_DISABLED: {
        auto env = TestHelper::getSystemEnv("ENABLE_COMPRESSION");
        if (!env.empty()) {
            if (env == "false") {
                return true;
            }
        }
    } break;
    case TokenType::SKIP_VECTOR_CAPACITY_TESTS: {
        if constexpr (VECTOR_CAPACITY_LOG_2 != STANDARD_VECTOR_CAPACITY_LOG_2) {
            return true;
        }
    } break;
    case TokenType::SKIP_NODE_GROUP_SIZE_TESTS: {
        if constexpr (StorageConfig::NODE_GROUP_SIZE_LOG2 != STANDARD_NODE_GROUP_SIZE_LOG_2) {
            return true;
        }
    } break;
    case TokenType::SKIP_PAGE_SIZE_TESTS: {
        if constexpr (PAGE_SIZE_LOG2 != STANDARD_PAGE_SIZE_LOG_2) {
            return true;
        }
    } break;
    case TokenType::SKIP_SEGMENT_SIZE_TESTS: {
        if constexpr (StorageConfig::MAX_SEGMENT_SIZE_LOG2 != STANDARD_MAX_SEGMENT_SIZE_LOG_2) {
            return true;
        }
    } break;
    case TokenType::SKIP_STATIC_LINK: {
#ifdef __STATIC_LINK_EXTENSION_TEST__
        return true;
#endif
    } break;
    default: {
        // DO NOTHING.
    }
    }
    // Move to the next line.
    return false;
}

void TestParser::parseHeader() {
    while (nextLine()) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::EMPTY: {
            // DO NOTHING.
        } break;
        case TokenType::DATASET: {
            checkMinimumParams(2);
            extractDataset();
        } break;
        case TokenType::BUFFER_POOL_SIZE: {
            checkMinimumParams(1);
            testGroup->bufferPoolSize = stoll(currentToken.params[1]);
        } break;
        case TokenType::TEST_FWD_ONLY_REL: {
            testGroup->testFwdOnly = true;
        } break;
        case TokenType::SEPARATOR: {
            return;
        }
        case TokenType::SKIP:
        case TokenType::SKIP_MUSL:
        case TokenType::SKIP_WASM:
        case TokenType::WASM_ONLY:
        case TokenType::SKIP_IN_MEM:
        case TokenType::SKIP_VECTOR_CAPACITY_TESTS:
        case TokenType::SKIP_NODE_GROUP_SIZE_TESTS:
        case TokenType::SKIP_SEGMENT_SIZE_TESTS:
        case TokenType::SKIP_COMPRESSION_DISABLED:
        case TokenType::SKIP_PAGE_SIZE_TESTS:
        case TokenType::SKIP_STATIC_LINK: {
            if (shouldSkip(currentToken.type)) {
                testGroup->group = "DISABLED_" + testGroup->group;
            }
        } break;
        default: {
            throw TestException("Invalid test header statement [" + path + ":" + line + "].");
        }
        }
    }
}

void TestParser::replaceVariables(std::string& str) const {
    for (auto& variable : variableMap) {
        StringUtils::replaceAll(str, "${" + variable.first + "}", variable.second.toString());
    }
}

void TestParser::extractExpectedResults(TestStatement& statement) {
    do {
        tokenize();
        if (currentToken.type == TokenType::EMPTY) {
            continue;
        }
        if (currentToken.type != TokenType::RESULT) {
            setCursorToPreviousLine();
            return;
        }
        statement.result.push_back(extractExpectedResultFromToken());
    } while (nextLine());
}

TestQueryResult TestParser::extractExpectedResultFromToken() {
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

void validateStatement(const TestStatement& statement, const std::string& path,
    const std::string& line) {
    if (statement.type != TestStatementType::INVALID) {
        throw TestException("Invalid statement [" + path + ":" + line + "].");
    }
}

TestStatement TestParser::parseStatement(const std::string& testCaseName) {
    TestStatement statement;
    while (true) {
        tokenize();
        switch (currentToken.type) {
        case TokenType::LOG: {
            validateStatement(statement, testCaseName, line);
            checkMinimumParams(1);
            statement.logMessage = paramsToString(1);
            replaceVariables(statement.logMessage);
            statement.type = TestStatementType::LOG;
            return statement;
        }
        case TokenType::BEGIN_CONCURRENT_EXECUTION: {
            statement.connectionsStatusFlag = ConcurrentStatusFlag::BEGIN;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::END_CONCURRENT_EXECUTION: {
            statement.connectionsStatusFlag = ConcurrentStatusFlag::END;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::SKIP_FSM_LEAK_CHECKER: {
            statement.skipFSMLeakCheckerFlag = true;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::RELOADDB: {
            statement.reloadDBFlag = true;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::CREATE_DATASET_SCHEMA: {
            validateStatement(statement, testCaseName, line);
            statement.dataset = getParam(1);
            statement.manualUseDataset = ManualUseDatasetFlag::SCHEMA;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::INSERT_DATASET_BY_ROW: {
            validateStatement(statement, testCaseName, line);
            statement.dataset = getParam(1);
            statement.manualUseDataset = ManualUseDatasetFlag::INSERT;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::IMPORT_DATABASE: {
            statement.importDBFlag = true;
            auto filePath = getParam(1);
            replaceVariables(filePath);
            statement.importFilePath = filePath;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::REMOVE_FILE: {
            statement.removeFileFlag = true;
            auto filePath = getParam(1);
            replaceVariables(filePath);
            statement.removeFilePath = filePath;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::MULTI_COPY_RANDOM: {
            statement.multiCopySplits = stoll(getParam(1));
            statement.multiCopyTable = getParam(2);
            int rest = 3;
            if (getParam(3) == "SEED") {
                statement.seed.resize(2);
                statement.seed[0] = stoll(getParam(4));
                statement.seed[1] = stoll(getParam(5));
                rest = 6;
            }
            auto multiCopySource = paramsToString(rest);
            replaceVariables(multiCopySource);
            statement.multiCopySource = multiCopySource;
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::SET_ENV: {
            auto envName = getParam(1);
            auto envValue = getParam(2);
#if defined(_WIN32)
            _putenv_s(envName.c_str(), envValue.c_str());
#else
            // NOLINTNEXTLINE(*-mt-unsafe)
            setenv(envName.c_str(), envValue.c_str(), 1 /* overwrite existing env*/);
#endif
            return statement;
        }
        case TokenType::SET: {
            checkMinimumParams(2);
            std::string varName = currentToken.params[1];
            Value value = parseAndEvaluateFunction();
            variableMap.insert_or_assign(varName, std::move(value));
            return statement;
        }
        case TokenType::CREATE_CONNECTION: {
            checkMinimumParams(1);
            testGroup->testCasesConnNames[testCaseName].insert(currentToken.params[1]);
            return statement;
        }

            // Following token types are not terminal for a statement, so we continue parsing.
        case TokenType::CHECKPOINT_WAIT_TIMEOUT: {
            checkMinimumParams(1);
            testGroup->checkpointWaitTimeout = stoi(currentToken.params[1]);
        } break;
        case TokenType::STATEMENT: {
            std::string query = paramsToString(1);
            extractConnName(query, statement);
            query += extractTextBeforeNextStatement(true);
            if (TestHelper::REWRITE_TESTS) {
                // Save the original query string before replacing variables. Needed to match the
                // statement in the file again.
                statement.originalQuery = query;
            }
            replaceVariables(query);
            statement.query = query;
        } break;
        case TokenType::BATCH_STATEMENTS: {
            std::string query = paramsToString(1);
            extractConnName(query, statement);
            statement.batchStatementsCSVFile = TestHelper::appendKuzuRootPath(
                (std::filesystem::path(TestHelper::TEST_STATEMENTS_PATH) / query.substr(7))
                    .string());
        } break;
        case TokenType::RESULT: {
            extractExpectedResults(statement);
            statement.type = TestStatementType::VALID;
            return statement;
        }
        case TokenType::CHECK_ORDER: {
            statement.checkOutputOrder = true;
        } break;
        case TokenType::CHECK_PRECISION: {
            statement.checkPrecision = true;
        } break;
        case TokenType::CHECK_COLUMN_NAMES: {
            statement.checkColumnNames = true;
        } break;
        case TokenType::EMPTY: {
            // DO NOTHING.
        } break;
        default: {
            throw TestException("Invalid statement [" + path + ":" + line + "].");
        }
        }
        nextLine();
    }
}

void TestParser::extractStatementBlock() {
    const std::string blockName = currentToken.params[1];
    while (nextLine()) {
        tokenize();
        if (currentToken.type == TokenType::END_OF_STATEMENT_BLOCK) {
            break;
        }
        auto statement = parseStatement(blockName);
        if (statement.isValid()) {
            statement.isPartOfStatementBlock = true;
            testGroup->testCasesStatementBlocks[blockName].push_back(std::move(statement));
            testGroup->testCasesConnNames[blockName] = std::set<std::string>();
        }
    }
}

Value TestParser::parseAndEvaluateFunction() {
    auto funcName = currentToken.params[2];
    // REPEAT 3 "col${count}, " = "col0, col1, col2, "
    if (StringUtils::caseInsensitiveEquals(funcName, REPEAT_FUNC)) {
        checkMinimumParams(4);
        return evaluateRepeat();
    }
    // ARANGE 0 3 = [0,1,2,3]
    if (funcName == ARANGE_FUNC) {
        checkMinimumParams(4);
        return evaluateARange();
    }

    // Handle function calls
    if (StringUtils::caseInsensitiveEquals(funcName, CURRENT_TIMESTAMP_FUNC)) {
        return evaluateCurrentTimestamp();
    }

    // Handle dot notation functions
    if (funcName.starts_with("random.")) {
        return evaluateRandomDotFunction(funcName);
    }

    // Handle quoted strings
    if (funcName.front() == '"' && funcName.back() == '"') {
        return Value(funcName.substr(1, funcName.size() - 2));
    }

    // Handle numeric literals
    try {
        return Value(static_cast<int64_t>(stoll(funcName)));
    } catch (const std::exception&) {
        throw TestException("Invalid numeric literal [" + funcName + "] in SET statement.");
    }
}

Value TestParser::evaluateRepeat() {
    const int times = stoi(currentToken.params[3]);
    std::string result;
    const std::string repeatString = StringUtils::extractStringBetween(paramsToString(4), '"', '"');
    if (repeatString.empty()) {
        throw TestException("Invalid SET data type [" + path + ":" + line + "].");
    }
    for (auto i = 1; i <= times; i++) {
        auto stringToAppend = repeatString;
        StringUtils::replaceAll(stringToAppend, "${count}", std::to_string(i));
        result += stringToAppend;
    }
    return Value(result);
}

Value TestParser::evaluateARange() const {
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
    return Value(result);
}

Value TestParser::evaluateCurrentTimestamp() {
    timestamp_t timestamp = Timestamp::getCurrentTimestamp();
    return Value(timestamp);
}

Value TestParser::evaluateRandom(const std::string& funcCall) {
    // Extract parameter from random(param)
    std::string param = funcCall.substr(7, funcCall.length() - 8); // Remove "random(" and ")"

    if (param.empty()) {
        // random() without parameter - return random integer 0-2^32
        return Value(randomEngine.nextRandomInteger());
    }
    // random(max) - return random integer between 0 and max-1
    int32_t maxVal = 0;
    try {
        maxVal = static_cast<int32_t>(stoi(param));
    } catch (const std::exception&) {
        throw TestException("Invalid random parameter [" + path + ":" + line + "].");
    }
    if (maxVal == 0) {
        throw TestException("Random max value must be positive [" + path + ":" + line + "].");
    }
    return Value(randomEngine.nextRandomInteger(maxVal));
}

Value TestParser::evaluateRandomDotFunction(const std::string& funcCall) {
    // Handle random.set_seed(value) and random.randInt32(max)
    if (funcCall.starts_with("random.set_seed(") && funcCall.ends_with(")")) {
        // Extract parameter from random.set_seed(param)
        std::string param =
            funcCall.substr(16, funcCall.length() - 17); // Remove "random.set_seed(" and ")"
        // Handle variable substitution if needed
        replaceVariables(param);
        try {
            uint64_t seedValue = static_cast<uint64_t>(stoll(param));
            randomEngine.setSeed(seedValue);
            return Value(0); // Return dummy value for _ variable
        } catch (const std::exception&) {
            throw TestException("Invalid seed parameter [" + path + ":" + line + "].");
        }
    }
    if (funcCall.starts_with("random.randInt32(") && funcCall.ends_with(")")) {
        // Extract parameter from random.randInt32(max)
        std::string param =
            funcCall.substr(17, funcCall.length() - 18); // Remove "random.randInt32(" and ")"
        int32_t maxVal = 0;
        try {
            maxVal = static_cast<int32_t>(stoi(param));
        } catch (const std::exception&) {
            throw TestException("Invalid random parameter [" + path + ":" + line + "].");
        }
        if (maxVal == 0) {
            throw TestException("Random max value must be positive [" + path + ":" + line + "].");
        }
        return Value(static_cast<int32_t>(randomEngine.nextRandomInteger(maxVal)));
    }
    throw TestException("Unknown random function [" + path + ":" + line + "].");
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
        } break;
        case TokenType::DEFINE_STATEMENT_BLOCK: {
            checkMinimumParams(2);
            extractStatementBlock();
        } break;
        case TokenType::INSERT_STATEMENT_BLOCK: {
            checkMinimumParams(1);
            addStatementBlock(currentToken.params[1], testCaseName);
        } break;
        case TokenType::LOAD_DYNAMIC_EXTENSION: {
            checkMinimumParams(1);
#ifndef __STATIC_LINK_EXTENSION_TEST__
            TestStatement loadExtensionStatement;
            auto extensionName = currentToken.params[1];
            loadExtensionStatement.connName = TestHelper::DEFAULT_CONN_NAME;
            loadExtensionStatement.query =
                stringFormat("LOAD EXTENSION '{}/extension/{}/build/lib{}.kuzu_extension'",
                    KUZU_ROOT_DIRECTORY, extensionName, extensionName);
            loadExtensionStatement.logMessage = "Dynamic load extension: " + extensionName;
            loadExtensionStatement.testResultType = ResultType::OK;
            loadExtensionStatement.result.emplace_back(ResultType::OK, 0,
                std::vector<std::string>{});
            testGroup->testCases[testCaseName].push_back(std::move(loadExtensionStatement));
            testGroup->testCasesConnNames[testCaseName].insert(TestHelper::DEFAULT_CONN_NAME);
#endif
        } break;
        case TokenType::TEST_FWD_ONLY_REL: {
            testFwdOnly = true;
        } break;
        case TokenType::EMPTY: {
            // DO NOTHING.
        } break;
        case TokenType::LOOP: {
            checkMinimumParams(3);
            parseLoop(testCaseName);
        } break;
        case TokenType::SKIP:
        case TokenType::SKIP_MUSL:
        case TokenType::SKIP_WASM:
        case TokenType::WASM_ONLY:
        case TokenType::SKIP_IN_MEM:
        case TokenType::SKIP_VECTOR_CAPACITY_TESTS:
        case TokenType::SKIP_NODE_GROUP_SIZE_TESTS:
        case TokenType::SKIP_PAGE_SIZE_TESTS:
        case TokenType::SKIP_COMPRESSION_DISABLED:
        case TokenType::SKIP_STATIC_LINK: {
            if (shouldSkip(currentToken.type)) {
                testCaseName = "DISABLED_" + testCaseName;
            }
        } break;
        default: {
            if (TestHelper::getSystemEnv("DEFAULT_REL_STORAGE_DIRECTION") == "fwd" &&
                !testFwdOnly && !testCaseName.starts_with("DISABLED_")) {
                testCaseName = "DISABLED_" + testCaseName;
            }
            // if it's not a special case, then it has to be a statement
            auto statement = parseStatement(testCaseName);
            if (statement.isValid()) {
                testGroup->testCases[testCaseName].push_back(std::move(statement));
                testGroup->testCasesConnNames[testCaseName].insert(TestHelper::DEFAULT_CONN_NAME);
            }
        }
        }
    }
}

void TestParser::addStatementBlock(const std::string& blockName,
    const std::string& testCaseName) const {
    if (testGroup->testCasesStatementBlocks.contains(blockName)) {
        for (const auto& statementPtr : testGroup->testCasesStatementBlocks[blockName]) {
            testGroup->testCases[testCaseName].push_back(TestStatement(statementPtr));
            testGroup->testCasesConnNames[testCaseName] = testGroup->testCasesConnNames[blockName];
        }
    } else {
        throw TestException("Statement block not found [" + path + ":" + blockName + "].");
    }
}

TokenType getTokenType(const std::string& input) {
    const auto iter = TOKEN_MAP.find(input);
    if (iter != TOKEN_MAP.end()) {
        return iter->second;
    }
    return TokenType::_SKIP_LINE;
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

void TestParser::parseLoop(const std::string& testCaseName) {
    const std::string loopVariable = currentToken.params[1];
    std::vector<std::string> loopValues;

    // Determine loop type based on parameter pattern
    if (currentToken.params.size() == 3 && currentToken.params[2].starts_with("[") &&
        currentToken.params[2].ends_with("]")) {
        // Array format: -LOOP var [val1,val2,val3]
        std::string arrayStr = currentToken.params[2];
        arrayStr = arrayStr.substr(1, arrayStr.length() - 2); // Remove [ ]

        std::stringstream ss(arrayStr);
        std::string value;
        while (std::getline(ss, value, ',')) {
            // Trim whitespace
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            if (!value.empty()) {
                loopValues.push_back(value);
            }
        }
    } else if (currentToken.params.size() >= 4) {
        // Range format: -LOOP var start end [step]
        int start = stoi(currentToken.params[2]);
        int end = stoi(currentToken.params[3]);
        int step = (currentToken.params.size() > 4) ? stoi(currentToken.params[4]) : 1;

        if (step <= 0) {
            throw TestException("Loop step must be positive [" + path + ":" + line + "].");
        }

        for (int i = start; i <= end; i += step) {
            loopValues.push_back(std::to_string(i));
        }
    } else {
        throw TestException("Invalid loop syntax [" + path + ":" + line + "].");
    }

    auto loopBeginFilePosition = getFilePosition();
    // Generate statements for each iteration
    for (const auto& value : loopValues) {
        setToPosition(loopBeginFilePosition);
        while (nextLine()) {
            tokenize();
            if (currentToken.type == TokenType::LOOP) {
                continue;
            }
            if (currentToken.type == TokenType::ENDLOOP) {
                break;
            }
            // Set loop variable value
            variableMap.insert_or_assign(loopVariable, Value(value));

            auto statement = parseStatement(testCaseName);
            if (statement.isValid()) {
                testGroup->testCases[testCaseName].push_back(std::move(statement));
                testGroup->testCasesConnNames[testCaseName].insert(TestHelper::DEFAULT_CONN_NAME);
            }
        }
    }
}

void TestParser::extractConnName(std::string& query, TestStatement& statement) {
    const std::regex pattern(R"(\[(conn.*?)\]\s*(.*))");
    std::smatch matches;
    bool statement_status = false;
    if (std::regex_search(query, matches, pattern)) {
        if (matches.size() == 3) {
            statement_status = true;
            statement.connName = matches[1];
            query = matches[2];
        }
    }
    if (!statement_status) {
        statement.connName = TestHelper::DEFAULT_CONN_NAME;
    }
}

} // namespace testing
} // namespace kuzu
