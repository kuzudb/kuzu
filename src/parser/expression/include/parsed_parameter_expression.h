#pragma once

#include "parsed_expression.h"

namespace graphflow {
namespace parser {

class ParsedParameterExpression : public ParsedExpression {

public:
    explicit ParsedParameterExpression(string parameterName, string raw)
        : ParsedExpression{PARAMETER, move(raw)}, parameterName{move(parameterName)} {}

    inline string getParameterName() const { return parameterName; }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               parameterName == ((ParsedParameterExpression&)other).parameterName;
    }

private:
    string parameterName;
};

} // namespace parser
} // namespace graphflow
