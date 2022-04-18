#pragma once

#include "parsed_expression.h"

namespace graphflow {
namespace parser {

class ParsedFunctionExpression : public ParsedExpression {

public:
    ParsedFunctionExpression(string functionName, string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, move(rawName)}, isDistinct{isDistinct}, functionName{move(
                                                                                 functionName)} {}

    ParsedFunctionExpression(string functionName, unique_ptr<ParsedExpression> child,
        string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, move(child), move(rawName)}, isDistinct{isDistinct},
          functionName{move(functionName)} {}

    ParsedFunctionExpression(string functionName, unique_ptr<ParsedExpression> left,
        unique_ptr<ParsedExpression> right, string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, move(left), move(right), move(rawName)},
          isDistinct{isDistinct}, functionName{move(functionName)} {}

    bool getIsDistinct() const { return isDistinct; }

    string getFunctionName() const { return functionName; }

    // A function might have more than 2 parameters.
    void addChild(unique_ptr<ParsedExpression> child) { children.push_back(move(child)); }

    bool equals(const ParsedExpression& other) const override {
        auto& functionExpression = (ParsedFunctionExpression&)other;
        return ParsedExpression::equals(other) && isDistinct == functionExpression.isDistinct &&
               functionName == functionExpression.functionName;
    }

private:
    bool isDistinct;
    string functionName;
};

} // namespace parser
} // namespace graphflow
