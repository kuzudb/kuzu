#pragma once

#include <fstream>
#include <numeric>

#include "common/exception/test.h"
#include "test_helper/test_helper.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

enum class TokenType {
    // header
    DATASET,
    SKIP,
    SKIP_MUSL,
    SKIP_32BIT,
    SKIP_WASM,
    SKIP_IN_MEM,
    SKIP_VECTOR_CAPACITY_TESTS,
    TEST_FWD_ONLY_REL,
    // body
    BUFFER_POOL_SIZE,
    CASE,
    CHECK_COLUMN_NAMES,
    CHECK_PRECISION,
    CHECK_ORDER,
    COMMIT,
    DEFINE,
    DEFINE_STATEMENT_BLOCK,
    EMPTY,
    END_OF_STATEMENT_BLOCK,
    INSERT_STATEMENT_BLOCK,
    LOG,
    PARALLELISM,
    RESULT,
    ROLLBACK,
    SEPARATOR,
    STATEMENT,
    _SKIP_LINE,
    CREATE_DATASET_SCHEMA,
    INSERT_DATASET_BY_ROW,
    MULTI_COPY_RANDOM,
    SEED,

    CHECKPOINT_WAIT_TIMEOUT,
    CREATE_CONNECTION,
    RELOADDB,
    BATCH_STATEMENTS,
    SET,
    BEGIN_CONCURRENT_EXECUTION,
    END_CONCURRENT_EXECUTION,

    // special for testing exporting database
    IMPORT_DATABASE,
    REMOVE_FILE
};

const std::unordered_map<std::string, TokenType> tokenMap = {{"-DATASET", TokenType::DATASET},
    {"-CASE", TokenType::CASE}, {"-COMMIT", TokenType::COMMIT},
    {"-CHECK_ORDER", TokenType::CHECK_ORDER}, {"-LOG", TokenType::LOG},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-PARALLELISM", TokenType::PARALLELISM}, {"-SKIP", TokenType::SKIP},
    {"-SKIP_MUSL", TokenType::SKIP_MUSL}, {"-SKIP_LINE", TokenType::DEFINE},
    {"-SKIP_32BIT", TokenType::SKIP_32BIT}, {"-SKIP_WASM", TokenType::SKIP_WASM},
    {"-SKIP_IN_MEM", TokenType::SKIP_IN_MEM},
    {"-SKIP_VECTOR_CAPACITY_TESTS", TokenType::SKIP_VECTOR_CAPACITY_TESTS},
    {"-TEST_FWD_ONLY_REL", TokenType::TEST_FWD_ONLY_REL}, {"-DEFINE", TokenType::DEFINE},
    {"-STATEMENT", TokenType::STATEMENT},
    {"-INSERT_STATEMENT_BLOCK", TokenType::INSERT_STATEMENT_BLOCK},
    {"-ROLLBACK", TokenType::ROLLBACK}, {"-BUFFER_POOL_SIZE", TokenType::BUFFER_POOL_SIZE},
    {"-CHECKPOINT_WAIT_TIMEOUT", TokenType::CHECKPOINT_WAIT_TIMEOUT},
    {"-BATCH_STATEMENTS", TokenType::BATCH_STATEMENTS}, {"-RELOADDB", TokenType::RELOADDB},
    {"-CREATE_CONNECTION", TokenType::CREATE_CONNECTION}, {"]", TokenType::END_OF_STATEMENT_BLOCK},
    {"----", TokenType::RESULT}, {"--", TokenType::SEPARATOR}, {"#", TokenType::EMPTY},
    {"-SET", TokenType::SET}, {"-IMPORT_DATABASE", TokenType::IMPORT_DATABASE},
    {"-REMOVE_FILE", TokenType::REMOVE_FILE}, {"-CHECK_PRECISION", TokenType::CHECK_PRECISION},
    {"-CHECK_COLUMN_NAMES", TokenType::CHECK_COLUMN_NAMES},
    {"-BEGIN_CONCURRENT_EXECUTION", TokenType::BEGIN_CONCURRENT_EXECUTION},
    {"-END_CONCURRENT_EXECUTION", TokenType::END_CONCURRENT_EXECUTION},
    {"-CREATE_DATASET_SCHEMA", TokenType::CREATE_DATASET_SCHEMA},
    {"-INSERT_DATASET_BY_ROW", TokenType::INSERT_DATASET_BY_ROW},
    {"-MULTI_COPY_RANDOM", TokenType::MULTI_COPY_RANDOM}};

class LogicToken {
public:
    TokenType type;
    std::vector<std::string> params;
};

class TestParser {
public:
    explicit TestParser(const std::string& path)
        : path{path}, testGroup{std::make_unique<TestGroup>()}, currentToken{} {}
    std::unique_ptr<TestGroup> parseTestFile();

private:
    std::string path;
    std::ifstream fileStream;
    std::streampos previousFilePosition;
    std::string line;
    std::string logMessage;
    std::unique_ptr<TestGroup> testGroup;
    std::string extractTextBeforeNextStatement(bool ignoreLineBreak = false);
    std::string parseCommand();
    std::string parseCommandArange() const;
    std::string parseCommandRepeat();
    LogicToken currentToken;

    void openFile();
    void tokenize();
    void genGroupName() const;
    void parseHeader();
    void parseBody();
    void extractExpectedResults(TestStatement* statement);
    TestQueryResult extractExpectedResultFromToken(bool checkOutputOrder);
    void extractStatementBlock();
    void extractDataset();
    void addStatementBlock(const std::string& blockName, const std::string& testCaseName) const;
    void replaceVariables(std::string& str) const;
    static void extractConnName(std::string& query, TestStatement* statement);

    void setCursorToPreviousLine() { fileStream.seekg(previousFilePosition); }

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

    TestStatement* extractStatement(TestStatement* statement, const std::string& testCaseName);
    TestStatement* addNewStatement(const std::string& testGroupName) const;

    const std::string exportDBPath = TestHelper::getTempDir("export_db");
    // Any value here will be replaced inside the .test files
    // in queries/statements and expected error message.
    // Example: ${KUZU_ROOT_DIRECTORY} will be replaced by
    // KUZU_ROOT_DIRECTORY
    std::unordered_map<std::string, std::string> variableMap = {
        {"KUZU_ROOT_DIRECTORY", KUZU_ROOT_DIRECTORY}, {"KUZU_VERSION", common::KUZU_VERSION},
        {"KUZU_EXPORT_DB_DIRECTORY", exportDBPath}};
};

} // namespace testing
} // namespace kuzu
