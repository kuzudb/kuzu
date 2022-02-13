#pragma once

#include "parsed_expression.h"

namespace graphflow {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {

public:
    ParsedPropertyExpression(
        ExpressionType type, string propertyName, unique_ptr<ParsedExpression> child, string raw)
        : ParsedExpression{type, move(child), move(raw)}, propertyName{move(propertyName)} {}

    inline string getPropertyName() const { return propertyName; }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               propertyName == ((ParsedPropertyExpression&)other).propertyName;
    }

private:
    string propertyName;
};

} // namespace parser
} // namespace graphflow
