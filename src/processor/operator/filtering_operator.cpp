#include "processor/operator/filtering_operator.h"

namespace kuzu {
namespace processor {

void SelVectorOverWriter::restoreSelVector(shared_ptr<SelectionVector>& selVector) {
    if (prevSelVector != nullptr) {
        selVector = prevSelVector;
    }
}

void SelVectorOverWriter::saveSelVector(shared_ptr<SelectionVector>& selVector) {
    if (prevSelVector == nullptr) {
        prevSelVector = selVector;
    }
    resetToCurrentSelVector(selVector);
}

void SelVectorOverWriter::resetToCurrentSelVector(shared_ptr<SelectionVector>& selVector) {
    currentSelVector->selectedSize = selVector->selectedSize;
    if (selVector->isUnfiltered()) {
        currentSelVector->resetSelectorToUnselected();
    } else {
        memcpy(currentSelVector->getSelectedPositionsBuffer(), selVector->selectedPositions,
            selVector->selectedSize * sizeof(sel_t));
        currentSelVector->resetSelectorToValuePosBuffer();
    }
    selVector = currentSelVector;
}

} // namespace processor
} // namespace kuzu
