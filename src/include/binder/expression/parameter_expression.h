#pragma once

#include "common/types/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {

public:
    explicit ParameterExpression(const std::string& parameterName, std::shared_ptr<common::Value> value)
        : Expression{common::PARAMETER, common::ANY, "$" + parameterName /* add $ to avoid conflict between parameter name and variable name */}, value{std::move(value)} {}

    inline void setDataType(const common::DataType& targetType) {
        assert(dataType.typeID == common::ANY);
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline std::shared_ptr<common::Value> getLiteral() const { return value; }

private:
    std::shared_ptr<common::Value> value;
};

} // namespace binder
} // namespace kuzu
