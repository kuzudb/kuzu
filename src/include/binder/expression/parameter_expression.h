#pragma once

#include "common/types/value/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {
public:
    explicit ParameterExpression(
        const std::string& parameterName, std::shared_ptr<common::Value> value)
        : Expression{common::ExpressionType::PARAMETER,
              common::LogicalType(common::LogicalTypeID::ANY), createUniqueName(parameterName)},
          parameterName(parameterName), value{std::move(value)} {}

    inline void setDataType(const common::LogicalType& targetType) {
        KU_ASSERT(dataType.getLogicalTypeID() == common::LogicalTypeID::ANY);
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline std::shared_ptr<common::Value> getLiteral() const { return value; }

    inline std::string toStringInternal() const final { return "$" + parameterName; }

private:
    inline static std::string createUniqueName(const std::string& input) { return "$" + input; }

private:
    std::string parameterName;
    std::shared_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
