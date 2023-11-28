#include "processor/operator/ddl/add_property.h"

#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

common::ValueVector* AddProperty::getDefaultValVector() {
    defaultValueEvaluator->evaluate();
    return defaultValueEvaluator->resultVector.get();
}

} // namespace processor
} // namespace kuzu
