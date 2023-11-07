#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedFunctionExpression : public ParsedExpression {
public:
    ParsedFunctionExpression(std::string functionName, std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> child,
        std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(child), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> left,
        std::unique_ptr<ParsedExpression> right, std::string rawName, bool isDistinct = false)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(left), std::move(right),
              std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string alias, std::string rawName,
        parsed_expression_vector children, bool isDistinct, std::string functionName)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(alias), std::move(rawName),
              std::move(children)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(bool isDistinct, std::string functionName)
        : ParsedExpression{common::ExpressionType::FUNCTION}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    inline bool getIsDistinct() const { return isDistinct; }

    inline std::string getFunctionName() const { return functionName; }

    // A function might have more than 2 parameters.
    inline void addChild(std::unique_ptr<ParsedExpression> child) {
        children.push_back(std::move(child));
    }

    static std::unique_ptr<ParsedFunctionExpression> deserialize(
        common::Deserializer& deserializer);

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedFunctionExpression>(
            alias, rawName, copyChildren(), isDistinct, functionName);
    }

private:
    void serializeInternal(common::Serializer& serializer) const override;

private:
    bool isDistinct;
    std::string functionName;
};

} // namespace parser
} // namespace kuzu
