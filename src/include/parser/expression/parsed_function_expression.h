#pragma once

#include "common/string_utils.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

struct ParsedFuncExprData {
    virtual ~ParsedFuncExprData() = default;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ParsedFuncExprData&, const TARGET&>(*this);
    }

    virtual std::unique_ptr<ParsedFuncExprData> copy() {
        return std::make_unique<ParsedFuncExprData>();
    }
};

struct ParsedLambdaExprData : public ParsedFuncExprData {
    std::unique_ptr<ParsedExpression> parsedExpr;

    explicit ParsedLambdaExprData(std::unique_ptr<ParsedExpression> parsedExpr)
        : parsedExpr{std::move(parsedExpr)} {}

    std::unique_ptr<ParsedFuncExprData> copy() override {
        return std::make_unique<ParsedLambdaExprData>(parsedExpr->copy());
    }
};

class ParsedFunctionExpression : public ParsedExpression {
public:
    ParsedFunctionExpression(std::string functionName, std::string rawName,
        std::unique_ptr<ParsedLambdaExprData> lambdaExprData)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(rawName)}, isDistinct{false},
          functionName{std::move(functionName)}, lambdaExprData{std::move(lambdaExprData)} {}

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

    // For clone only.
    ParsedFunctionExpression(std::string alias, std::string rawName, parsed_expr_vector children,
        std::string functionName, bool isDistinct,
        std::unique_ptr<ParsedFuncExprData> lambdaExprData)
        : ParsedExpression{common::ExpressionType::FUNCTION, std::move(alias), std::move(rawName),
              std::move(children)},
          isDistinct{isDistinct}, functionName{std::move(functionName)},
          lambdaExprData{std::move(lambdaExprData)} {}

    ParsedFunctionExpression(std::string functionName, bool isDistinct)
        : ParsedExpression{common::ExpressionType::FUNCTION}, isDistinct{isDistinct},
          functionName{std::move(functionName)} {}

    bool getIsDistinct() const { return isDistinct; }

    std::string getFunctionName() const { return functionName; }
    std::string getNormalizedFunctionName() const {
        return common::StringUtils::getUpper(functionName);
    }

    // A function might have more than 2 parameters.
    void addChild(std::unique_ptr<ParsedExpression> child) { children.push_back(std::move(child)); }

    static std::unique_ptr<ParsedFunctionExpression> deserialize(
        common::Deserializer& deserializer);

    const ParsedFuncExprData* getParsedFuncExprData() const { return lambdaExprData.get(); }

    std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedFunctionExpression>(alias, rawName, copyChildren(),
            functionName, isDistinct, lambdaExprData->copy());
    }

private:
    void serializeInternal(common::Serializer& serializer) const override;

private:
    bool isDistinct;
    std::string functionName;
    std::unique_ptr<ParsedFuncExprData> lambdaExprData;
};

} // namespace parser
} // namespace kuzu
