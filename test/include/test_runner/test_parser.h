#include <fstream>
#include <numeric>

#include "common/exception/test.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

enum class TokenType {
    // header
    DATASET,
    GROUP,
    SKIP,
    // body
    BEGIN_READ_ONLY_TRANSACTION,
    BEGIN_WRITE_TRANSACTION,
    BUFFER_POOL_SIZE,
    CASE,
    CHECK_ORDER,
    COMMIT,
    DEFINE,
    DEFINE_STATEMENT_BLOCK,
    EMPTY,
    ENCODED_JOIN,
    END_OF_STATEMENT_BLOCK,
    ENUMERATE,
    INSERT_STATEMENT_BLOCK,
    LOG,
    PARALLELISM,
    RESULT,
    ROLLBACK,
    SEPARATOR,
    STATEMENT,
    _SKIP_LINE,

    CHECKPOINT_WAIT_TIMEOUT,
    CREATE_CONNECTION,
    RELOADDB,
    BATCH_STATEMENTS
};

const std::unordered_map<std::string, TokenType> tokenMap = {{"-GROUP", TokenType::GROUP},
    {"-DATASET", TokenType::DATASET}, {"-CASE", TokenType::CASE}, {"-COMMIT", TokenType::COMMIT},
    {"-CHECK_ORDER", TokenType::CHECK_ORDER}, {"-ENCODED_JOIN", TokenType::ENCODED_JOIN},
    {"-LOG", TokenType::LOG}, {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-ENUMERATE", TokenType::ENUMERATE},
    {"-BEGIN_WRITE_TRANSACTION", TokenType::BEGIN_WRITE_TRANSACTION},
    {"-BEGIN_READ_ONLY_TRANSACTION", TokenType::BEGIN_READ_ONLY_TRANSACTION},
    {"-PARALLELISM", TokenType::PARALLELISM}, {"-SKIP", TokenType::SKIP},
    {"-DEFINE", TokenType::DEFINE}, {"-STATEMENT", TokenType::STATEMENT},
    {"-INSERT_STATEMENT_BLOCK", TokenType::INSERT_STATEMENT_BLOCK},
    {"-ROLLBACK", TokenType::ROLLBACK}, {"-BUFFER_POOL_SIZE", TokenType::BUFFER_POOL_SIZE},
    {"-CHECKPOINT_WAIT_TIMEOUT", TokenType::CHECKPOINT_WAIT_TIMEOUT},
    {"-BATCH_STATEMENTS", TokenType::BATCH_STATEMENTS}, {"-RELOADDB", TokenType::RELOADDB},
    {"-CREATE_CONNECTION", TokenType::CREATE_CONNECTION}, {"]", TokenType::END_OF_STATEMENT_BLOCK},
    {"----", TokenType::RESULT}, {"--", TokenType::SEPARATOR}, {"#", TokenType::EMPTY}};

class LogicToken {
public:
    TokenType type;
    std::vector<std::string> params;
};

class TestParser {
public:
    explicit TestParser(const std::string& path)
        : testGroup{std::make_unique<TestGroup>()}, path{path} {}
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
    std::string parseCommandArange();
    std::string parseCommandRepeat();
    LogicToken currentToken;

    void openFile();
    void tokenize();
    void parseHeader();
    void parseBody();
    void extractExpectedResult(TestStatement* statement);
    void extractStatementBlock();
    void extractDataset();
    void addStatementBlock(const std::string& blockName, const std::string& testGroupName);
    void replaceVariables(std::string& str);
    void extractConnName(std::string& query, TestStatement* statement);

    inline bool endOfFile() { return fileStream.eof(); }
    inline void setCursorToPreviousLine() { fileStream.seekg(previousFilePosition); }

    inline bool nextLine() {
        previousFilePosition = fileStream.tellg();
        return static_cast<bool>(getline(fileStream, line));
    }

    inline void checkMinimumParams(int minimumParams) {
        if (currentToken.params.size() - 1 < minimumParams) {
            throw common::TestException("Minimum number of parameters is " +
                                        std::to_string(minimumParams) + ". [" + path + ":" + line +
                                        "]");
        }
    }

    inline std::string paramsToString(int startParamIdx) {
        return std::accumulate(std::next(currentToken.params.begin(), startParamIdx),
            currentToken.params.end(), std::string(),
            [](const std::string& a, const std::string& b) {
                return a + (a.empty() ? "" : " ") + b;
            });
    }

    TestStatement* extractStatement(
        TestStatement* currentStatement, const std::string& testCaseName);
    TestStatement* addNewStatement(std::string& name);

    // Any value here will be replaced inside the .test files
    // in queries/statements and expected error message.
    // Example: ${KUZU_ROOT_DIRECTORY} will be replaced by
    // KUZU_ROOT_DIRECTORY
    std::unordered_map<std::string, std::string> variableMap = {
        {"KUZU_ROOT_DIRECTORY", KUZU_ROOT_DIRECTORY}, {"KUZU_VERSION", common::KUZU_VERSION}};
};

} // namespace testing
} // namespace kuzu
