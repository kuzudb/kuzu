#pragma once

#include "common/types/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {
public:
    explicit ParameterExpression(
        const std::string& parameterName, std::shared_ptr<common::Value> value)
        : Expression{common::PARAMETER, common::ANY, createUniqueName(parameterName)},
          parameterName(parameterName), value{std::move(value)} {}

    inline void setDataType(const common::DataType& targetType) {
        assert(dataType.typeID == common::ANY);
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline std::shared_ptr<common::Value> getLiteral() const { return value; }

    std::string toString() const override { return "$" + parameterName; }

private:
    inline static std::string createUniqueName(const std::string& input) { return "$" + input; }

private:
    std::string parameterName;
    std::shared_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
