#pragma once

#include "src/antlr4/CypherParser.h"

namespace graphflow {
namespace parser {

class GraphflowCypherParser : public CypherParser {

public:
    explicit GraphflowCypherParser(antlr4::TokenStream* input) : CypherParser(input) {}

    void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) override;

    void notifyNodePatternWithoutParentheses(
        std::string nodeName, antlr4::Token* startToken) override;

    void notifyInvalidNotEqualOperator(antlr4::Token* startToken) override;

    void notifyEmptyToken(antlr4::Token* startToken) override;

    void notifyReturnNotAtEnd(antlr4::Token* startToken) override;

    void notifyNonBinaryComparison(antlr4::Token* startToken) override;
};

} // namespace parser
} // namespace graphflow
