#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {

public:
    ParsedPropertyExpression(
        ExpressionType type, string propertyName, unique_ptr<ParsedExpression> child, string raw)
        : ParsedExpression{type, move(child), move(raw)}, propertyName{move(propertyName)} {}

    inline string getPropertyName() const { return propertyName; }

private:
    string propertyName;
};

} // namespace parser
} // namespace kuzu
