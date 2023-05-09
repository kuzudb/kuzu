#include <cstring>

#include "common/file_utils.h"
#include "main/kuzu.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

enum class TokenType {
    GROUP,
    TEST,
    CASE,
    NAME,
    DATASET,
    CHECK_ORDER,
    READ_ONLY,
    SKIP,
    STATEMENT,
    STATEMENT_BLOCK,
    DEFINE_STATEMENT_BLOCK,
    QUERY,
    LOOP,
    FOREACH,
    PARALLELISM,
    RESULT
};

const std::unordered_map<std::string, TokenType> tokenMap = {
    {"#", TokenType::SKIP},
    {"-GROUP", TokenType::GROUP},
    {"-TEST", TokenType::TEST},
    {"-CASE", TokenType::CASE},
    {"-NAME", TokenType::NAME},
    {"-DATASET", TokenType::DATASET},
    {"-CHECK_ORDER", TokenType::CHECK_ORDER},
    {"-READ_ONLY", TokenType::READ_ONLY},
    {"-SKIP", TokenType::SKIP},
    {"-STATEMENT", TokenType::STATEMENT},
    {"-STATEMENT_BLOCK", TokenType::STATEMENT_BLOCK},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-QUERY", TokenType::QUERY},
    {"-LOOP", TokenType::LOOP},
    {"-FOREACH", TokenType::FOREACH},
    {"-PARALLELISM", TokenType::PARALLELISM},
    {"----", TokenType::RESULT}
};

class LogicToken {
public:
    TokenType type;
    std::vector<std::string> params;
};

class TestParser {
public:
    std::string line;
    void tokenize();
    std::vector<std::string> splitString(const std::string& input);
    LogicToken currentToken;
    void openFile(const std::string &path);
    bool nextLine();


private:
    std::ifstream fileStream;
};



} // namespace testing
} // namespace kuzu
