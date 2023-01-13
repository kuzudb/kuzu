#pragma once

#include "common/types/value.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {

public:
    explicit ParameterExpression(const string& parameterName, shared_ptr<Value> value)
        : Expression{PARAMETER, ANY, "$" + parameterName /* add $ to avoid conflict between parameter name and variable name */}, value{std::move(value)} {}

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == ANY);
        dataType = targetType;
        value->setDataType(targetType);
    }

    inline shared_ptr<Value> getLiteral() const { return value; }

private:
    shared_ptr<Value> value;
};

} // namespace binder
} // namespace kuzu
