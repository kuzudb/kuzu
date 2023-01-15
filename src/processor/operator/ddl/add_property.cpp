#include "processor/operator/ddl/add_property.h"

namespace kuzu {
namespace processor {

void AddProperty::executeDDLInternal() {
    expressionEvaluator->evaluate();
    catalog->addProperty(tableID, propertyName, dataType);
}

uint8_t* AddProperty::getDefaultVal() {
    auto expressionVector = expressionEvaluator->resultVector;
    assert(expressionEvaluator->resultVector->dataType == dataType);
    auto posInExpressionVector = expressionVector->state->selVector->selectedPositions[0];
    return expressionVector->getData() +
           expressionVector->getNumBytesPerValue() * posInExpressionVector;
}

} // namespace processor
} // namespace kuzu
