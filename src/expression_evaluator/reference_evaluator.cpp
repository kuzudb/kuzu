#include "expression_evaluator/reference_evaluator.h"

using namespace kuzu::common;

namespace kuzu {
namespace evaluator {

inline static bool isTrue(ValueVector& vector, uint64_t pos) {
    assert(vector.dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    return !vector.isNull(pos) && vector.getValue<bool>(pos);
}

bool ReferenceExpressionEvaluator::select(SelectionVector& selVector) {
    uint64_t numSelectedValues = 0;
    auto selectedBuffer = resultVector->state->selVector->getSelectedPositionsBuffer();
    if (resultVector->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
            selectedBuffer[numSelectedValues] = i;
            numSelectedValues += isTrue(*resultVector, i);
        }
    } else {
        for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
            auto pos = resultVector->state->selVector->selectedPositions[i];
            selectedBuffer[numSelectedValues] = pos;
            numSelectedValues += isTrue(*resultVector, pos);
        }
    }
    selVector.selectedSize = numSelectedValues;
    return numSelectedValues > 0;
}

} // namespace evaluator
} // namespace kuzu
