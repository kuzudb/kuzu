#include <cstring>

#include "common/file_utils.h"
#include "main/kuzu.h"
#include "test_runner/test_case.h"

using namespace kuzu::main;

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
    TestParser() : testCase(std::make_unique<TestCase>()) {}

    std::unique_ptr<TestCase> parseTestFile(const std::string& path);

private:
    std::ifstream fileStream;
    std::string line;
    std::string name;
    LogicToken currentToken;
    std::unique_ptr<TestCase> testCase;

    std::string paramsToString();

    void openFile(const std::string& path);

    void tokenize();

    void parseHeader();

    void parseBody();

    void extractExpectedResult(TestStatement* currentStatement);

    void extractStatementBlock();

    inline bool endOfFile() { return fileStream.eof(); }

    inline bool nextLine() { return static_cast<bool>(getline(fileStream, line)); }

    inline void checkMinimumParams(int minimumParams) {
        if (currentToken.params.size() < minimumParams) {
            throw common::Exception("Invalid number of parameters for statement [" + line + "]");
        }
    }

    TestStatement* extractStatement(TestStatement* currentStatement);

    TestStatement* addNewStatement(std::vector<std::unique_ptr<TestStatement>>& statements);
};

} // namespace testing
} // namespace kuzu
