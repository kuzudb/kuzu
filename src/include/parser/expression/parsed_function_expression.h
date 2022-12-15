#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedFunctionExpression : public ParsedExpression {
public:
    ParsedFunctionExpression(string functionName, string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, std::move(rawName)}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    ParsedFunctionExpression(string functionName, unique_ptr<ParsedExpression> child,
        string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, std::move(child), std::move(rawName)}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    ParsedFunctionExpression(string functionName, unique_ptr<ParsedExpression> left,
        unique_ptr<ParsedExpression> right, string rawName, bool isDistinct = false)
        : ParsedExpression{FUNCTION, std::move(left), std::move(right), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    inline bool getIsDistinct() const { return isDistinct; }

    inline string getFunctionName() const { return functionName; }

    // A function might have more than 2 parameters.
    inline void addChild(unique_ptr<ParsedExpression> child) {
        children.push_back(std::move(child));
    }

private:
    bool isDistinct;
    string functionName;
};

} // namespace parser
} // namespace kuzu
