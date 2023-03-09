#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(common::ExpressionType type, std::string propertyName,
        std::unique_ptr<ParsedExpression> child, std::string raw)
        : ParsedExpression{type, std::move(child), std::move(raw)}, propertyName{
                                                                        std::move(propertyName)} {}

    inline std::string getPropertyName() const { return propertyName; }

private:
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
