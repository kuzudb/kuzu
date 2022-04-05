#pragma once

#include "expression.h"

#include "src/common/types/include/literal.h"

namespace graphflow {
namespace binder {

class ParameterExpression : public Expression {

public:
    explicit ParameterExpression(const string& parameterName, shared_ptr<Literal> literal)
        : Expression{PARAMETER, INVALID, "$" + parameterName /* add $ to avoid conflict between parameter name and variable name */}, literal{move(literal)} {}

    inline void setDataType(const DataType& targetType) {
        assert(this->dataType.typeID == INVALID);
        this->dataType = targetType;
        literal->dataType = targetType;
    }

    inline shared_ptr<Literal> getLiteral() const { return literal; }

private:
    shared_ptr<Literal> literal;
};

} // namespace binder
} // namespace graphflow
