#pragma once

#include "common/types/value.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
public:
    ParsedLiteralExpression(std::unique_ptr<common::Value> value, std::string raw)
        : ParsedExpression{common::LITERAL, std::move(raw)}, value{std::move(value)} {}

    ParsedLiteralExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children, std::unique_ptr<common::Value> value)
        : ParsedExpression{type, std::move(alias), std::move(rawName), std::move(children)},
          value{std::move(value)} {}

    inline common::Value* getValue() const { return value.get(); }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedLiteralExpression>(
            type, alias, rawName, copyChildren(), value->copy());
    }

private:
    std::unique_ptr<common::Value> value;
};

} // namespace parser
} // namespace kuzu
