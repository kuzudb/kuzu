#pragma once

#include "expression.h"

#include "src/common/include/configs.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(DataType dataType, const string& propertyName, uint32_t propertyID,
        const shared_ptr<Expression>& child)
        : Expression{PROPERTY, move(dataType), child, child->getUniqueName() + "." + propertyName},
          propertyName{propertyName}, propertyID{propertyID} {}

    inline string getPropertyName() const { return propertyName; }

    inline uint32_t getPropertyID() const { return propertyID; }

    inline bool isInternalID() const { return getPropertyName() == INTERNAL_ID_SUFFIX; }

private:
    string propertyName;
    uint32_t propertyID;
};

} // namespace binder
} // namespace kuzu
