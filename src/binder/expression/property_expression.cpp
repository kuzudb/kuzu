#include "include/property_expression.h"

namespace graphflow {
namespace binder {

PropertyExpression::PropertyExpression(DataType dataType, const string& propertyName,
    uint32_t propertyKey, const shared_ptr<Expression>& child)
    : Expression{PROPERTY, move(dataType)}, propertyName{propertyName}, propertyKey{propertyKey} {
    uniqueName = child->getUniqueName() + "." + propertyName;
    children.push_back(child);
}

} // namespace binder
} // namespace graphflow
