#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class VariableExpression : public Expression {
public:
    VariableExpression(
        common::LogicalType dataType, std::string uniqueName, std::string variableName)
        : Expression{common::VARIABLE, dataType, std::move(uniqueName)}, variableName{std::move(
                                                                             variableName)} {}

    inline std::string toStringInternal() const final { return variableName; }

private:
    std::string variableName;
};

} // namespace binder
} // namespace kuzu
