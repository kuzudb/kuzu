#include "processor/operator/filtering_operator.h"

namespace kuzu {
namespace processor {

void FilteringOperator::restoreSelVector(
    SelectionVector* prevSelVector, SelectionVector* selVector) {
    if (prevSelVector->selectedPositions == nullptr) {
        return;
    }
    selVector->selectedSize = prevSelVector->selectedSize;
    if (prevSelVector->isUnfiltered()) {
        selVector->resetSelectorToUnselected();
    } else {
        auto sizeToCopy = prevSelVector->selectedSize * sizeof(sel_t);
        selVector->resetSelectorToValuePosBuffer();
        memcpy(
            selVector->selectedPositions, prevSelVector->getSelectedPositionsBuffer(), sizeToCopy);
    }
}

void FilteringOperator::saveSelVector(SelectionVector* prevSelVector, SelectionVector* selVector) {
    prevSelVector->selectedSize = selVector->selectedSize;
    if (selVector->isUnfiltered()) {
        prevSelVector->resetSelectorToUnselected();
    } else {
        auto sizeToCopy = prevSelVector->selectedSize * sizeof(sel_t);
        memcpy(prevSelVector->getSelectedPositionsBuffer(), selVector->getSelectedPositionsBuffer(),
            sizeToCopy);
        prevSelVector->resetSelectorToValuePosBuffer();
    }
}

} // namespace processor
} // namespace kuzu
