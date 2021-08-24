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

    unique_ptr<Expression> copy() override {
        return make_unique<PropertyExpression>(dataType, propertyName, propertyKey, children[0]->copy());
    }

public:
    string propertyName;
    uint32_t propertyKey;
};

} // namespace binder
} // namespace graphflow
