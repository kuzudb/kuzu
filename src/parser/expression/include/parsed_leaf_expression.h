#pragma once

#include "parsed_expression.h"

namespace graphflow {
namespace parser {

// Leaf expression can be variable or literal
class ParsedLeafExpression : public ParsedExpression {

public:
    ParsedLeafExpression(ExpressionType type, string variableName, string raw)
        : ParsedExpression{type, move(raw)}, variableName{move(variableName)} {}

    inline string getVariableName() const { return variableName; }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               variableName == ((ParsedLeafExpression&)other).variableName;
    }

private:
    string variableName;
};

} // namespace parser
} // namespace graphflow
