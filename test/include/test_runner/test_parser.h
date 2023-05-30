#include <cstring>

#include "common/file_utils.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

enum class TokenType {
    GROUP,
    DATASET,
    TEST,
    BEGIN_WRITE_TRANSACTION,
    BUFFER_POOL_SIZE,
    CASE,
    CHECK_ORDER,
    DEFINE_STATEMENT_BLOCK,
    EMPTY,
    ENCODED_JOIN,
    END_OF_STATEMENT_BLOCK,
    ENUMERATE,
    NAME,
    PARALLELISM,
    QUERY,
    READ_ONLY,
    RESULT,
    SEPARATOR,
    SKIP,
    STATEMENT,
    STATEMENT_BLOCK
};

const std::unordered_map<std::string, TokenType> tokenMap = {{"-GROUP", TokenType::GROUP},
    {"-TEST", TokenType::TEST}, {"-DATASET", TokenType::DATASET}, {"-CASE", TokenType::CASE},
    {"-CHECK_ORDER", TokenType::CHECK_ORDER}, {"-ENCODED_JOIN", TokenType::ENCODED_JOIN},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-ENUMERATE", TokenType::ENUMERATE}, {"-NAME", TokenType::NAME},
    {"-BEGIN_WRITE_TRANSACTION", TokenType::BEGIN_WRITE_TRANSACTION},
    {"-PARALLELISM", TokenType::PARALLELISM}, {"-QUERY", TokenType::QUERY},
    {"-READ_ONLY", TokenType::READ_ONLY}, {"-SKIP", TokenType::SKIP},
    {"-STATEMENT", TokenType::STATEMENT}, {"-STATEMENT_BLOCK", TokenType::STATEMENT_BLOCK},
    {"-BUFFER_POOL_SIZE", TokenType::BUFFER_POOL_SIZE}, {"]", TokenType::END_OF_STATEMENT_BLOCK},
    {"----", TokenType::RESULT}, {"--", TokenType::SEPARATOR}, {"#", TokenType::EMPTY}};

const std::unordered_map<std::string, std::string> variableMap = {
    {"${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY}};

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
    std::string paramsToString();
    std::string extractTextBeforeNextStatement();
    LogicToken currentToken;
    void openFile();
    void tokenize();
    void parseHeader();
    void parseBody();
    void extractExpectedResult(TestStatement* currentStatement);
    void extractStatementBlock();
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
    TestStatement* extractStatement(TestStatement* currentStatement);
    TestStatement* addNewStatement(std::string& name);
};

} // namespace testing
} // namespace kuzu
