#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class PropertyExpression : public Expression {

public:
    PropertyExpression(string propertyVariableName, DataType dataType, uint32_t propertyKey,
        shared_ptr<Expression> child)
        : Expression{PROPERTY, dataType, move(child)}, propertyKey{propertyKey} {
        variableName = move(propertyVariableName);
    }

public:
    uint32_t propertyKey;
};

} // namespace binder
} // namespace graphflow
