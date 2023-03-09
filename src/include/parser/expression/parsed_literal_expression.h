#pragma once

#include "common/types/value.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
public:
    ParsedLiteralExpression(std::unique_ptr<common::Value> value, std::string raw)
        : ParsedExpression{common::LITERAL, std::move(raw)}, value{std::move(value)} {}

    inline common::Value* getValue() const { return value.get(); }

private:
    std::unique_ptr<common::Value> value;
};

} // namespace parser
} // namespace kuzu
