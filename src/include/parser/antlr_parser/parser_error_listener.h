#pragma once

#include <string>

#include "antlr4-runtime.h"

using namespace std;
using namespace antlr4;

namespace kuzu {
namespace parser {

class ParserErrorListener : public BaseErrorListener {

public:
    void syntaxError(Recognizer* recognizer, Token* offendingSymbol, size_t line,
        size_t charPositionInLine, const std::string& msg, std::exception_ptr e) override;

private:
    string formatUnderLineError(Recognizer& recognizer, const Token& offendingToken, size_t line,
        size_t charPositionInLine);
};

} // namespace parser
} // namespace kuzu
