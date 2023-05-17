#include <cstring>

#include "common/file_utils.h"
#include "main/kuzu.h"

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
    END_OF_STATEMENT_BLOCK,
    ENUMERATE,
    FOREACH,
    LOOP,
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
    {"-CHECK_ORDER", TokenType::CHECK_ORDER},
    {"-DEFINE_STATEMENT_BLOCK", TokenType::DEFINE_STATEMENT_BLOCK},
    {"-ENUMERATE", TokenType::ENUMERATE}, {"-FOREACH", TokenType::FOREACH},
    {"-LOOP", TokenType::LOOP}, {"-NAME", TokenType::NAME},
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
    std::string line;
    void tokenize();
    std::vector<std::string> splitString(const std::string& input);
    LogicToken currentToken;
    void openFile(const std::string& path);
    std::string paramsToString();

    inline std::string getParam(int param) {
        return currentToken.params[param];
        if (param > currentToken.params.size()) {
            return "";
        }
    }

    inline bool endOfFile() { return fileStream.eof(); }

    inline bool nextLine() { return static_cast<bool>(getline(fileStream, line)); }

private:
    std::ifstream fileStream;
};

} // namespace testing
} // namespace kuzu
