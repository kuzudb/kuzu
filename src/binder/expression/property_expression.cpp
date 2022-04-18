#include "include/property_expression.h"

namespace graphflow {
namespace binder {

PropertyExpression::PropertyExpression(DataType dataType, const string& propertyName,
    uint32_t propertyKey, const shared_ptr<Expression>& child)
    : Expression{PROPERTY, move(dataType), child, child->getUniqueName() + "." + propertyName},
      propertyName{propertyName}, propertyKey{propertyKey} {}

} // namespace binder
} // namespace graphflow
