#pragma once

#include "common/string_utils.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedFunctionExpression : public ParsedExpression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::FUNCTION;

public:
    ParsedFunctionExpression(std::string functionName, std::string rawName, bool isDistinct = false)
        : ParsedExpression{expressionType_, std::move(rawName)}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> child,
        std::string rawName, bool isDistinct = false)
        : ParsedExpression{expressionType_, std::move(child), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, std::unique_ptr<ParsedExpression> left,
        std::unique_ptr<ParsedExpression> right, std::string rawName, bool isDistinct = false)
        : ParsedExpression{expressionType_, std::move(left), std::move(right), std::move(rawName)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string alias, std::string rawName, parsed_expr_vector children,
        std::string functionName, bool isDistinct)
        : ParsedExpression{expressionType_, std::move(alias), std::move(rawName),
              std::move(children)},
          isDistinct{isDistinct}, functionName{std::move(functionName)} {}

    ParsedFunctionExpression(std::string functionName, bool isDistinct)
        : ParsedExpression{expressionType_}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    bool getIsDistinct() const { return isDistinct; }

    std::string getFunctionName() const { return functionName; }
    std::string getNormalizedFunctionName() const {
        return common::StringUtils::getUpper(functionName);
    }

    void addChild(std::unique_ptr<ParsedExpression> child) { children.push_back(std::move(child)); }

    static std::unique_ptr<ParsedFunctionExpression> deserialize(
        common::Deserializer& deserializer);

    std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedFunctionExpression>(alias, rawName, copyVector(children),
            functionName, isDistinct);
    }

private:
    void serializeInternal(common::Serializer& serializer) const override;

private:
    bool isDistinct;
    std::string functionName;
};

} // namespace parser
} // namespace kuzu
