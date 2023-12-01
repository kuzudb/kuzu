#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class VariableExpression : public Expression {
public:
    VariableExpression(
        common::LogicalType dataType, std::string uniqueName, std::string variableName)
        : Expression{common::ExpressionType::VARIABLE, std::move(dataType), std::move(uniqueName)},
          variableName{std::move(variableName)} {}

    inline std::string getVariableName() const { return variableName; }

    inline std::string toStringInternal() const final { return variableName; }

    inline std::unique_ptr<Expression> copy() const final {
        return std::make_unique<VariableExpression>(*dataType.copy(), uniqueName, variableName);
    }

private:
    std::string variableName;
};

} // namespace binder
} // namespace kuzu
