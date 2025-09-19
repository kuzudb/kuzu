#pragma once

#include <fstream>
#include <numeric>

#include "common/exception/test.h"
#include "common/random_engine.h"
#include "test_helper/test_helper.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {
enum class TokenType {
    // header only
    DATASET,
    BUFFER_POOL_SIZE,

    // Skip or enable tests
    SKIP,
    SKIP_MUSL,
    SKIP_WASM,
    SKIP_STATIC_LINK,
    WASM_ONLY,
    SKIP_IN_MEM,
    SKIP_VECTOR_CAPACITY_TESTS,
    SKIP_NODE_GROUP_SIZE_TESTS,
    SKIP_PAGE_SIZE_TESTS,
    SKIP_SEGMENT_SIZE_TESTS,
    SKIP_COMPRESSION_DISABLED,
    TEST_FWD_ONLY_REL,

    // Test case statements
    CASE,
    CHECK_COLUMN_NAMES,
    CHECK_PRECISION,
    CHECK_ORDER,
    DEFINE_STATEMENT_BLOCK,
    EMPTY,
    END_OF_STATEMENT_BLOCK,
    INSERT_STATEMENT_BLOCK,
    LOG,
    RESULT,
    SEPARATOR,
    SET,
    SET_ENV,
    STATEMENT,
    _SKIP_LINE,
    CREATE_DATASET_SCHEMA,
    INSERT_DATASET_BY_ROW,
    MULTI_COPY_RANDOM,
    LOAD_DYNAMIC_EXTENSION,
    SEED,
    // SKIP_FSM_LEAK_CHECKER works only per-test for now, not test-file-wide.
    SKIP_FSM_LEAK_CHECKER,

    CHECKPOINT_WAIT_TIMEOUT,
    CREATE_CONNECTION,
    RELOADDB,
    BATCH_STATEMENTS,
    BEGIN_CONCURRENT_EXECUTION,
    END_CONCURRENT_EXECUTION,

    // Special tokens for testing exporting database
    IMPORT_DATABASE,
    REMOVE_FILE,

    // Loop control tokens
    LOOP,
    ENDLOOP
};

const std::unordered_map<std::string, TokenType> TOKEN_MAP = {{"-DATASET", TokenType::DATASET},
    {"-CASE", TokenType::CASE}, {"-CHECK_ORDER", TokenType::CHECK_ORDER}, {"-LOG", TokenType::LOG},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK}, {"-SKIP", TokenType::SKIP},
    {"-SKIP_MUSL", TokenType::SKIP_MUSL}, {"-SKIP_LINE", TokenType::SET},
    {"-SKIP_WASM", TokenType::SKIP_WASM},
    {"-LOAD_DYNAMIC_EXTENSION", TokenType::LOAD_DYNAMIC_EXTENSION},
    {"-SKIP_STATIC_LINK", TokenType::SKIP_STATIC_LINK}, {"-WASM_ONLY", TokenType::WASM_ONLY},
    {"-SKIP_IN_MEM", TokenType::SKIP_IN_MEM},
    {"-SKIP_VECTOR_CAPACITY_TESTS", TokenType::SKIP_VECTOR_CAPACITY_TESTS},
    {"-SKIP_NODE_GROUP_SIZE_TESTS", TokenType::SKIP_NODE_GROUP_SIZE_TESTS},
    {"-SKIP_PAGE_SIZE_TESTS", TokenType::SKIP_PAGE_SIZE_TESTS},
    {"-SKIP_SEGMENT_SIZE_TESTS", TokenType::SKIP_SEGMENT_SIZE_TESTS},
    {"-SKIP_COMPRESSION_DISABLED", TokenType::SKIP_COMPRESSION_DISABLED},
    {"-TEST_FWD_ONLY_REL", TokenType::TEST_FWD_ONLY_REL}, {"-STATEMENT", TokenType::STATEMENT},
    {"-INSERT_STATEMENT_BLOCK", TokenType::INSERT_STATEMENT_BLOCK},
    {"-BUFFER_POOL_SIZE", TokenType::BUFFER_POOL_SIZE},
    {"-CHECKPOINT_WAIT_TIMEOUT", TokenType::CHECKPOINT_WAIT_TIMEOUT},
    {"-BATCH_STATEMENTS", TokenType::BATCH_STATEMENTS}, {"-RELOADDB", TokenType::RELOADDB},
    {"-CREATE_CONNECTION", TokenType::CREATE_CONNECTION}, {"]", TokenType::END_OF_STATEMENT_BLOCK},
    {"----", TokenType::RESULT}, {"--", TokenType::SEPARATOR}, {"#", TokenType::EMPTY},
    {"-SET", TokenType::SET}, {"-SET_ENV", TokenType::SET_ENV},
    {"-IMPORT_DATABASE", TokenType::IMPORT_DATABASE}, {"-REMOVE_FILE", TokenType::REMOVE_FILE},
    {"-CHECK_PRECISION", TokenType::CHECK_PRECISION},
    {"-CHECK_COLUMN_NAMES", TokenType::CHECK_COLUMN_NAMES},
    {"-BEGIN_CONCURRENT_EXECUTION", TokenType::BEGIN_CONCURRENT_EXECUTION},
    {"-END_CONCURRENT_EXECUTION", TokenType::END_CONCURRENT_EXECUTION},
    {"-CREATE_DATASET_SCHEMA", TokenType::CREATE_DATASET_SCHEMA},
    {"-INSERT_DATASET_BY_ROW", TokenType::INSERT_DATASET_BY_ROW},
    {"-MULTI_COPY_RANDOM", TokenType::MULTI_COPY_RANDOM}, {"-LOOP", TokenType::LOOP},
    {"-ENDLOOP", TokenType::ENDLOOP}, {"-SKIP_FSM_LEAK_CHECK", TokenType::SKIP_FSM_LEAK_CHECKER}};

class LogicToken {
public:
    TokenType type;
    std::vector<std::string> params;
};

class TestParser {
public:
    static constexpr uint64_t STANDARD_VECTOR_CAPACITY_LOG_2 = 11;
    static constexpr uint64_t STANDARD_NODE_GROUP_SIZE_LOG_2 = 17;
    static constexpr uint64_t STANDARD_PAGE_SIZE_LOG_2 = 12;
    static constexpr uint64_t STANDARD_MAX_SEGMENT_SIZE_LOG_2 = 18;

    explicit TestParser(std::string path);
    std::unique_ptr<TestGroup> parseTestFile();

private:
    std::string extractTextBeforeNextStatement(bool ignoreLineBreak = false);
    common::Value parseAndEvaluateFunction();
    common::Value evaluateARange() const;
    common::Value evaluateRepeat();
    static common::Value evaluateCurrentTimestamp();
    common::Value evaluateRandom(const std::string& funcCall);
    common::Value evaluateRandomDotFunction(const std::string& funcCall);

    void openFile();
    void tokenize();
    void genGroupName() const;
    void parseHeader();
    void parseBody();
    void parseLoop(const std::string& testCaseName);
    void extractExpectedResults(TestStatement& statement);
    TestQueryResult extractExpectedResultFromToken();
    void extractStatementBlock();
    void extractDataset();
    void addStatementBlock(const std::string& blockName, const std::string& testCaseName) const;
    void replaceVariables(std::string& str) const;
    static void extractConnName(std::string& query, TestStatement& statement);

    void setCursorToPreviousLine() { fileStream.seekg(previousFilePosition); }
    void setToPosition(uint64_t position) { fileStream.seekg(position); }
    uint64_t getFilePosition() { return fileStream.tellg(); }

    bool nextLine() {
        previousFilePosition = fileStream.tellg();
        return static_cast<bool>(getline(fileStream, line));
    }

    void checkMinimumParams(uint64_t minimumParams) const {
        if (currentToken.params.size() - 1 < minimumParams) {
            throw common::TestException("Minimum number of parameters is " +
                                        std::to_string(minimumParams) + ". [" + path + ":" + line +
                                        "]");
        }
    }

    std::string paramsToString(int startParamIdx) {
        return std::accumulate(std::next(currentToken.params.begin(), startParamIdx),
            currentToken.params.end(), std::string(),
            [](const std::string& a, const std::string& b) {
                return a + (a.empty() ? "" : " ") + b;
            });
    }

    std::string getParam(int paramIdx) { return currentToken.params[paramIdx]; }

    static bool shouldSkip(TokenType type);
    TestStatement parseStatement(const std::string& testCaseName);

private:
    const std::string exportDBPath = TestHelper::getTempDir("export_db");
    const std::string REPEAT_FUNC = "REPEAT";
    const std::string ARANGE_FUNC = "ARANGE";
    const std::string CURRENT_TIMESTAMP_FUNC = "current_timestamp()";
    const std::string RANDOM_FUNC = "RANDOM";
    // Any value here will be replaced inside the .test files
    // in queries/statements and expected error message.
    // Example: ${KUZU_ROOT_DIRECTORY} will be replaced by
    // KUZU_ROOT_DIRECTORY
    std::unordered_map<std::string, common::Value> variableMap;

private:
    std::string path;
    std::ifstream fileStream;
    std::streampos previousFilePosition;
    std::string line;
    std::string logMessage;
    std::unique_ptr<TestGroup> testGroup;
    LogicToken currentToken;
    common::RandomEngine randomEngine;
};

} // namespace testing
} // namespace kuzu
