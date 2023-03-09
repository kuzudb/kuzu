#pragma once

#include <string>

#include "antlr4-runtime.h"

namespace kuzu {
namespace parser {

class ParserErrorListener : public antlr4::BaseErrorListener {

public:
    void syntaxError(antlr4::Recognizer* recognizer, antlr4::Token* offendingSymbol, size_t line,
        size_t charPositionInLine, const std::string& msg, std::exception_ptr e) override;

private:
    std::string formatUnderLineError(antlr4::Recognizer& recognizer,
        const antlr4::Token& offendingToken, size_t line, size_t charPositionInLine);
};

} // namespace parser
} // namespace kuzu
