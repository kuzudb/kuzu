#pragma once

#include "expression.h"

#include "src/common/include/configs.h"

namespace graphflow {
namespace binder {

class PropertyExpression : public Expression {

public:
    PropertyExpression(DataType dataType, const string& propertyName, uint32_t propertyKey,
        const shared_ptr<Expression>& child);

    inline string getPropertyName() const { return propertyName; }

    inline uint32_t getPropertyKey() const { return propertyKey; }

    inline bool isInternalID() const { return getPropertyName() == INTERNAL_ID_SUFFIX; }

private:
    string propertyName;
    uint32_t propertyKey;
};

} // namespace binder
} // namespace graphflow
