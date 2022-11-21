#pragma once

#include "cypher_parser.h"

namespace kuzu {
namespace parser {

class KuzuCypherParser : public CypherParser {

public:
    explicit KuzuCypherParser(antlr4::TokenStream* input) : CypherParser(input) {}

    void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) override;

    void notifyNodePatternWithoutParentheses(
        std::string nodeName, antlr4::Token* startToken) override;

    void notifyInvalidNotEqualOperator(antlr4::Token* startToken) override;

    void notifyEmptyToken(antlr4::Token* startToken) override;

    void notifyReturnNotAtEnd(antlr4::Token* startToken) override;

    void notifyNonBinaryComparison(antlr4::Token* startToken) override;
};

} // namespace parser
} // namespace kuzu
