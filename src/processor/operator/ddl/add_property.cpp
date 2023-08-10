#include "processor/operator/ddl/add_property.h"

namespace kuzu {
namespace processor {

uint8_t* AddProperty::getDefaultVal() {
    defaultValueEvaluator->evaluate();
    auto expressionVector = defaultValueEvaluator->resultVector;
    assert(defaultValueEvaluator->resultVector->dataType == *dataType);
    auto posInExpressionVector = expressionVector->state->selVector->selectedPositions[0];
    return expressionVector->getData() +
           expressionVector->getNumBytesPerValue() * posInExpressionVector;
}

} // namespace processor
} // namespace kuzu
