#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class VariableExpression final : public Expression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::VARIABLE;

public:
    VariableExpression(common::LogicalType dataType, std::string uniqueName,
        std::string variableName)
        : Expression{expressionType_, std::move(dataType), std::move(uniqueName)},
          variableName{std::move(variableName)} {}

    std::string getVariableName() const { return variableName; }

    void cast(const common::LogicalType& type) override;

    std::string toStringInternal() const override { return variableName; }

    std::unique_ptr<Expression> copy() const override {
        return std::make_unique<VariableExpression>(dataType.copy(), uniqueName, variableName);
    }

private:
    std::string variableName;
};

} // namespace binder
} // namespace kuzu
