#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class LambdaParameterExpression final : public Expression {
public:
    LambdaParameterExpression(common::LogicalType type, const std::string& uniqueName)
        : Expression{common::ExpressionType::LAMBDA_PARAM, type.copy(), uniqueName} {}

    std::string toStringInternal() const override { return uniqueName; }

    std::unique_ptr<Expression> copy() const override {
        return std::make_unique<LambdaParameterExpression>(dataType.copy(), uniqueName);
    }
};

} // namespace binder
} // namespace kuzu
