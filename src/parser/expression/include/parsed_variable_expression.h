#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedVariableExpression : public ParsedExpression {

public:
    ParsedVariableExpression(string variableName, string raw)
        : ParsedExpression{VARIABLE, move(raw)}, variableName{move(variableName)} {}

    inline string getVariableName() const { return variableName; }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               variableName == ((ParsedVariableExpression&)other).variableName;
    }

private:
    string variableName;
};

} // namespace parser
} // namespace kuzu
