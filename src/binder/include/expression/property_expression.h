#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class PropertyExpression : public Expression {

public:
    PropertyExpression(DataType dataType, string propertyName, uint32_t propertyKey,
        const shared_ptr<Expression>& child)
        : Expression{PROPERTY, dataType, child}, propertyName{move(propertyName)},
          propertyKey{propertyKey} {}

    string getInternalName() const override {
        return children[0]->getInternalName() + "." + propertyName;
    }

public:
    string propertyName;
    uint32_t propertyKey;
};

} // namespace binder
} // namespace graphflow
