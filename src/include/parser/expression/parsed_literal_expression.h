#pragma once

#include "common/type_utils.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {

public:
    ParsedLiteralExpression(unique_ptr<Literal> literal, string raw)
        : ParsedExpression{LITERAL, move(raw)}, literal{move(literal)} {}

    inline Literal* getLiteral() const { return literal.get(); }

private:
    unique_ptr<Literal> literal;
};

} // namespace parser
} // namespace kuzu
