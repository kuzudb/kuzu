#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedFunctionExpression : public ParsedExpression {
public:
    ParsedFunctionExpression(std::string functionName, std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::FUNCTION, std::move(rawName)}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> child,
        std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::FUNCTION, std::move(child), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> left,
        std::unique_ptr<ParsedExpression> right, std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::FUNCTION, std::move(left), std::move(right), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children, bool isDistinct, std::string functionName)
        : ParsedExpression{type, std::move(alias), std::move(rawName), std::move(children)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    inline bool getIsDistinct() const { return isDistinct; }

    inline std::string getFunctionName() const { return functionName; }

    // A function might have more than 2 parameters.
    inline void addChild(std::unique_ptr<ParsedExpression> child) {
        children.push_back(std::move(child));
    }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedFunctionExpression>(
            type, alias, rawName, copyChildren(), isDistinct, functionName);
    }

private:
    bool isDistinct;
    std::string functionName;
};

} // namespace parser
} // namespace kuzu
