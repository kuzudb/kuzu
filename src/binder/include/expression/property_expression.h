#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class PropertyExpression : public Expression {

public:
    PropertyExpression(DataType dataType, const string& propertyName, uint32_t propertyKey,
        const shared_ptr<Expression>& child)
        : Expression{PROPERTY, dataType}, propertyName{propertyName}, propertyKey{propertyKey} {
        uniqueName = child->getUniqueName() + "." + propertyName;
        children.push_back(child);
    }

public:
    string propertyName;
    uint32_t propertyKey;
};

} // namespace binder
} // namespace graphflow
