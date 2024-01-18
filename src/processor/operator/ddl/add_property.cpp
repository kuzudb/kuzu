#include "processor/operator/ddl/add_property.h"

namespace kuzu {
namespace processor {

common::ValueVector* AddProperty::getDefaultValVector(ExecutionContext* context) {
    defaultValueEvaluator->evaluate(context->clientContext);
    return defaultValueEvaluator->resultVector.get();
}

} // namespace processor
} // namespace kuzu
