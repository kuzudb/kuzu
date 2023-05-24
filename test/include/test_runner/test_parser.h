#include <cstring>

#include "common/file_utils.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

enum class TokenType {
    GROUP,
    DATASET,
    TEST,
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
    {"-PARALLELISM", TokenType::PARALLELISM}, {"-QUERY", TokenType::QUERY},
    {"-READ_ONLY", TokenType::READ_ONLY}, {"-SKIP", TokenType::SKIP},
    {"-STATEMENT", TokenType::STATEMENT}, {"-STATEMENT_BLOCK", TokenType::STATEMENT_BLOCK},
    {"]", TokenType::END_OF_STATEMENT_BLOCK}, {"----", TokenType::RESULT},
    {"--", TokenType::SEPARATOR}, {"#", TokenType::EMPTY}};

class LogicToken {
public:
    TokenType type;
    std::vector<std::string> params;
};

class TestParser {
public:
    TestParser() : testGroup(std::make_unique<TestGroup>()) {}
    std::unique_ptr<TestGroup> parseTestFile(const std::string& path);

private:
    std::ifstream fileStream;
    std::string line;
    std::string name;
    std::unique_ptr<TestGroup> testGroup;
    std::string paramsToString();
    std::string extractTextBeforeNextStatement();
    LogicToken currentToken;
    void openFile(const std::string& path);
    void tokenize();
    void parseHeader();
    void parseBody();
    void extractExpectedResult(TestStatement* currentStatement);
    void extractStatementBlock();
    void addStatementBlock(const std::string& blockName, const std::string& testGroupName);
    inline bool endOfFile() { return fileStream.eof(); }
    inline bool nextLine() { return static_cast<bool>(getline(fileStream, line)); }
    inline void checkMinimumParams(int minimumParams) {
        if (currentToken.params.size() < minimumParams) {
            throw common::Exception("Invalid number of parameters for statement [" + line + "]");
        }
    }
    TestStatement* extractStatement(TestStatement* currentStatement);
    TestStatement* addNewStatement(std::string& name);
};

} // namespace testing
} // namespace kuzu
