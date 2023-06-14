#include <cstring>
#include <numeric>

#include "common/file_utils.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

enum class TokenType {
    /* header */
    BEGIN_WRITE_TRANSACTION,
    DATASET,
    GROUP,
    SKIP,
    /* body */
    BUFFER_POOL_SIZE,
    CASE,
    CHECK_ORDER,
    DEFINE,
    DEFINE_STATEMENT_BLOCK,
    EMPTY,
    ENCODED_JOIN,
    END_OF_STATEMENT_BLOCK,
    ENUMERATE,
    NAME,
    PARALLELISM,
    QUERY,
    RESULT,
    SEPARATOR,
    STATEMENT,
    STATEMENT_BLOCK,
    _SKIP_LINE
};

const std::unordered_map<std::string, TokenType> tokenMap = {{"-GROUP", TokenType::GROUP},
    {"-DATASET", TokenType::DATASET}, {"-CASE", TokenType::CASE},
    {"-CHECK_ORDER", TokenType::CHECK_ORDER}, {"-ENCODED_JOIN", TokenType::ENCODED_JOIN},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-ENUMERATE", TokenType::ENUMERATE}, {"-NAME", TokenType::NAME},
    {"-BEGIN_WRITE_TRANSACTION", TokenType::BEGIN_WRITE_TRANSACTION},
    {"-PARALLELISM", TokenType::PARALLELISM}, {"-QUERY", TokenType::QUERY},
    {"-SKIP", TokenType::SKIP}, {"-DEFINE", TokenType::DEFINE},
    {"-STATEMENT", TokenType::STATEMENT}, {"-STATEMENT_BLOCK", TokenType::STATEMENT_BLOCK},
    {"-BUFFER_POOL_SIZE", TokenType::BUFFER_POOL_SIZE}, {"]", TokenType::END_OF_STATEMENT_BLOCK},
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
    std::string name;
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

    TestStatement* extractStatement(TestStatement* currentStatement);
    TestStatement* addNewStatement(std::string& name);

    // Any value here will be replaced inside the .test files
    // in queries/statements and expected error message.
    // Example: ${KUZU_ROOT_DIRECTORY} will be replaced by
    // KUZU_ROOT_DIRECTORY
    std::unordered_map<std::string, std::string> variableMap = {
        {"KUZU_ROOT_DIRECTORY", KUZU_ROOT_DIRECTORY}};
};

} // namespace testing
} // namespace kuzu
