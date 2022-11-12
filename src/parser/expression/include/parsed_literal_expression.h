#pragma once

#include "parsed_expression.h"

#include "src/common/include/type_utils.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {

public:
    ParsedLiteralExpression(unique_ptr<Literal> literal, string raw)
        : ParsedExpression{LITERAL, move(raw)}, literal{move(literal)} {}

    inline Literal* getLiteral() const { return literal.get(); }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               TypeUtils::toString(*literal) ==
                   TypeUtils::toString(*((ParsedLiteralExpression&)other).literal);
    }

private:
    unique_ptr<Literal> literal;
};

} // namespace parser
} // namespace kuzu
