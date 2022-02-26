#pragma once

#include "parsed_expression.h"

namespace graphflow {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {

public:
    ParsedLiteralExpression(ExpressionType type, string valInStr, string raw)
        : ParsedExpression{type, move(raw)}, valInStr{move(valInStr)} {}

    inline string getValInStr() const { return valInStr; }

    bool equals(const ParsedExpression& other) const override {
        return ParsedExpression::equals(other) &&
               valInStr == ((ParsedLiteralExpression&)other).valInStr;
    }

private:
    string valInStr;
    vector<ParsedExpression> children;
};

} // namespace parser
} // namespace graphflow
