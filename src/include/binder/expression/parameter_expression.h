#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {
public:
    explicit ParameterExpression(const std::string& parameterName,
        std::shared_ptr<common::Value> value)
        : Expression{common::ExpressionType::PARAMETER, common::LogicalType(*value->getDataType()),
              createUniqueName(parameterName)},
          parameterName(parameterName), value{std::move(value)} {}

    void cast(const common::LogicalType& type) override;

    std::shared_ptr<common::Value> getLiteral() const { return value; }

private:
    std::string toStringInternal() const final { return "$" + parameterName; }
    static std::string createUniqueName(const std::string& input) { return "$" + input; }

private:
    std::string parameterName;
    std::shared_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
