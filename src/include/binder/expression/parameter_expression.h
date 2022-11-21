#pragma once

#include "common/types/literal.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {

public:
    explicit ParameterExpression(const string& parameterName, shared_ptr<Literal> literal)
        : Expression{PARAMETER, ANY, "$" + parameterName /* add $ to avoid conflict between parameter name and variable name */}, literal{move(literal)} {}

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == ANY);
        dataType = targetType;
        literal->dataType = targetType;
    }

    inline shared_ptr<Literal> getLiteral() const { return literal; }

private:
    shared_ptr<Literal> literal;
};

} // namespace binder
} // namespace kuzu
