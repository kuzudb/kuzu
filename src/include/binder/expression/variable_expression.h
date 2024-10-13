#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class VariableExpression : public Expression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::VARIABLE;

public:
    VariableExpression(common::LogicalType dataType, const std::string& uniqueName,
        const std::string& variableName)
        : Expression{expressionType_, std::move(dataType), uniqueName}, variableName{variableName} {
    }

    std::string getVariableName() const { return variableName; }

    void cast(const common::LogicalType& type) override;

    std::string toStringInternal() const final { return variableName; }

    std::unique_ptr<Expression> copy() const final {
        return std::make_unique<VariableExpression>(dataType.copy(), uniqueName, variableName);
    }

private:
    std::string variableName;
};

} // namespace binder
} // namespace kuzu
