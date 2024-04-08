#pragma once

#include "common/types/value/value.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
public:
    ParsedLiteralExpression(common::Value value, std::string raw)
        : ParsedExpression{common::ExpressionType::LITERAL, std::move(raw)},
          value{std::move(value)} {}

    ParsedLiteralExpression(std::string alias, std::string rawName, parsed_expr_vector children,
        common::Value value)
        : ParsedExpression{common::ExpressionType::LITERAL, std::move(alias), std::move(rawName),
              std::move(children)},
          value{std::move(value)} {}

    explicit ParsedLiteralExpression(common::Value value)
        : ParsedExpression{common::ExpressionType::LITERAL}, value{std::move(value)} {}

    inline const common::Value* getValue() const { return &value; }
    inline common::Value* getValueUnsafe() { return &value; }

    static inline std::unique_ptr<ParsedLiteralExpression> deserialize(
        common::Deserializer& deserializer) {
        return std::make_unique<ParsedLiteralExpression>(*common::Value::deserialize(deserializer));
    }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedLiteralExpression>(alias, rawName, copyChildren(), value);
    }

private:
    void serializeInternal(common::Serializer& serializer) const override {
        value.serialize(serializer);
    }

private:
    common::Value value;
};

} // namespace parser
} // namespace kuzu
