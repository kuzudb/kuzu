#pragma once

#include "common/types/value.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
public:
    ParsedLiteralExpression(unique_ptr<Value> value, string raw)
        : ParsedExpression{LITERAL, std::move(raw)}, value{std::move(value)} {}

    inline Value* getValue() const { return value.get(); }

private:
    unique_ptr<Value> value;
};

} // namespace parser
} // namespace kuzu
