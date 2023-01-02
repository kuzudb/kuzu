#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedParameterExpression : public ParsedExpression {

public:
    explicit ParsedParameterExpression(string parameterName, string raw)
        : ParsedExpression{PARAMETER, move(raw)}, parameterName{move(parameterName)} {}

    inline string getParameterName() const { return parameterName; }

private:
    string parameterName;
};

} // namespace parser
} // namespace kuzu
