#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(
        std::string propertyName, std::unique_ptr<ParsedExpression> child, std::string raw)
        : ParsedExpression{common::ExpressionType::PROPERTY, std::move(child), std::move(raw)},
          propertyName{std::move(propertyName)} {}

    inline std::string getPropertyName() const { return propertyName; }

private:
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
